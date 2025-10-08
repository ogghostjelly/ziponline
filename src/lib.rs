use std::{io, mem, num::ParseIntError};

use bytes::{Buf, Bytes};
use futures::{
    TryStreamExt, future,
    stream::{self, FuturesUnordered, TryChunksError},
};
use reqwest::{Client, Url, header::ToStrError};

use crate::{
    chunk_by::EvenChunkBy,
    parser::{Parser, ParserStream},
    ring_buf::RingBuffer,
    structs::{Cdfh, CompressionMethod, Eocd, Eocd32, Eocd64},
};

mod chunk_by;
mod parser;
mod ring_buf;
mod structs;

pub async fn extract_file(
    client: &Client,
    url: &Url,
    filesize: Option<usize>,
    filename: &str,
) -> Result<impl io::Read> {
    let start = std::time::Instant::now();
    let filesize = match filesize {
        Some(filesize) => filesize,
        None => request_content_length(client, url).await?,
    };
    println!(
        "Got Content-Length in {:?}",
        std::time::Instant::now() - start
    );

    let start = std::time::Instant::now();
    let Some(eocd) = request_eocd(client, url, filesize).await? else {
        return Err(Error::EocdNotFound);
    };
    println!("Got EOCD in {:?}", std::time::Instant::now() - start);

    let start = std::time::Instant::now();
    let Some(cdfh) = find_in_cd(client, url, &eocd, filename).await? else {
        return Err(Error::CdFileNotFound);
    };
    println!("Got CDFH in {:?}", std::time::Instant::now() - start);

    let start = std::time::Instant::now();
    let resp = client
        .get(url.clone())
        .header("Range", format!("bytes={}-", cdfh.file_header_offset))
        .send()
        .await?;

    let mut parser = Parser::new(resp.bytes_stream());
    let signature: [u8; 4] = parser.take_fixed_slice().await?;
    if signature != *b"PK\x03\x04" {
        return Err(Error::MalformedFileHeader);
    }

    let Some(()) = read_fh(&mut parser).await? else {
        return Err(Error::MalformedFileHeader);
    };

    let bytes = parser.take_bytes(cdfh.compressed_size as usize).await?;
    let reader = inflate::DeflateDecoder::new(bytes.reader());

    println!("Got File Header in {:?}", std::time::Instant::now() - start);

    Ok(reader)
}

async fn find_in_cd(
    client: &Client,
    url: &Url,
    eocd: &Eocd,
    filename: &str,
) -> Result<Option<Cdfh>> {
    // Sometimes the chunk boundaries will split a CDFH in half,
    // so we keep track of bytes that aren't part of any CDFH (stray bytes),
    // and join them back up at the end.

    // Process chunks
    let mut chunks_fut = FuturesUnordered::new();

    for (from, to) in EvenChunkBy::new(eocd.cd_size, 10) {
        let from = from + eocd.cd_offset;
        let to = to + eocd.cd_offset;

        chunks_fut.push(request_chunk(client, url, from, to, filename, eocd.offset));
    }

    // Process stray chunks
    // Join the starts and ends of stray bytes to create valid chunks.
    let mut joined_strays: Vec<Vec<u8>> = vec![];

    while let Some((start, end, cdfh)) = chunks_fut.try_next().await? {
        if let Some(cdfh) = cdfh {
            return Ok(Some(cdfh));
        }

        if joined_strays.is_empty() {
            // First element in the list, just push it right on.
            joined_strays.push(start);
        } else {
            // Join the start of these bytes to the end of the previous bytes.
            let last = joined_strays.len() - 1;
            let mut start = start;
            joined_strays[last].append(&mut start);
        }

        joined_strays.push(end);
    }

    let mut stray_chunks_fut = FuturesUnordered::new();

    // Process strays
    for bytes in joined_strays {
        let bytes = Bytes::from_owner(bytes);
        let parser = Parser::new(stream::once(future::ok(bytes)));
        stray_chunks_fut.push(process_chunk(parser, filename, eocd.offset));
    }

    while let Some((start, end, cdfh)) = stray_chunks_fut.try_next().await? {
        _ = (start, end); // start/end should be empty.

        if let Some(cdfh) = cdfh {
            return Ok(Some(cdfh));
        }
    }

    Ok(None)
}

async fn request_chunk(
    client: &Client,
    url: &Url,
    from: u64,
    to: u64,
    filename: &str,
    maximum_allowed_offset: usize,
) -> Result<(Vec<u8>, Vec<u8>, Option<Cdfh>)> {
    let resp = client
        .get(url.clone())
        .header("Range", format!("bytes={from}-{to}"))
        .send()
        .await?;

    let r = Parser::new(resp.bytes_stream());

    process_chunk(r, filename, maximum_allowed_offset).await
}

async fn process_chunk<S: ParserStream>(
    mut r: Parser<S>,
    filename: &str,
    maximum_allowed_offset: usize,
) -> Result<(Vec<u8>, Vec<u8>, Option<Cdfh>)> {
    let mut strays = Vec::new(); // stray bytes at the start of the stream
    let mut buf = RingBuffer::<4>::new();
    let mut start_stray = None;
    let mut found = None;

    while let Some(value) = r.next().await? {
        if let Some(x) = buf.push(value) {
            strays.push(x);
        }

        // Is it a CDFH?
        if buf.as_slice() == b"PK\x01\x02" {
            let cdfh = match read_cdfh(&mut r, maximum_allowed_offset).await {
                Ok(None) | Err(Error::UnexpectedEof) => continue,
                Ok(Some(cdfh)) => cdfh,
                Err(e) => return Err(e),
            };

            if let None = start_stray {
                start_stray = Some(mem::take(&mut strays));
            } else {
                strays.clear();
            }
            buf.clear();

            if cdfh.filename == filename {
                found = Some(cdfh);
                break;
            }
        }
    }

    for value in buf.as_slice() {
        strays.push(*value);
    }

    Ok((start_stray.unwrap_or_default(), strays, found))
}

/// Read a central directory file header or None if it is a false positive.
/// The given reader should return bytes right after the magic number `PK\x01\x02`.
async fn read_cdfh<S: ParserStream>(
    r: &mut Parser<S>,
    maximum_allowed_offset: usize,
) -> Result<Option<Cdfh>> {
    let version_made_by = r.take_u16().await?;
    let version_to_extract = r.take_u16().await?;
    // The version is stored in the last 8 bits of the field,
    // if the version is larger than 63 it's likely a false positive.
    if (version_made_by & 0xff) > 63 || (version_to_extract & 0xff) > 63 {
        return Ok(None);
    }
    let general_purpose_flags = r.take_u16().await?;
    let compression_method_id = r.take_u16().await?;
    let Some(compression_method) = CompressionMethod::from_id(compression_method_id) else {
        return Ok(None);
    };
    let last_modification_time = r.take_u16().await?;
    let last_modification_date = r.take_u16().await?;
    let crc32 = r.take_u32().await?;
    let compressed_size = r.take_u32().await?;
    let uncompressed_size = r.take_u32().await?;
    let filename_length = r.take_u16().await?;
    let extra_field_length = r.take_u16().await?;
    let file_comment_length = r.take_u16().await?;
    let disk_number = r.take_u16().await?;
    let internal_attrs = r.take_u16().await?;
    let external_attrs = r.take_u32().await?;
    let file_header_offset = r.take_u32().await?;
    if file_header_offset as usize > maximum_allowed_offset {
        return Ok(None);
    }

    // Filename should be valid UTF-8
    let Ok(filename) = String::from_utf8(r.take_bytes(filename_length as usize).await?.to_vec())
    else {
        return Ok(None);
    };
    let _extra_field = r.skip_bytes(extra_field_length as usize).await?;
    let _file_comment = r.skip_bytes(file_comment_length as usize).await?;

    Ok(Some(Cdfh {
        version_made_by,
        version_to_extract,
        general_purpose_flags,
        compression_method,
        last_modification_time,
        last_modification_date,
        crc32,
        compressed_size,
        uncompressed_size,
        extra_field_length,
        file_comment_length,
        disk_number,
        internal_attrs,
        external_attrs,
        file_header_offset,
        filename,
    }))
}

/// Read a file header or None if it is a false positive.
/// The given reader should return bytes right after the magic number `PK\x03\x04`.
async fn read_fh<S: ParserStream>(r: &mut Parser<S>) -> Result<Option<()>> {
    let version_to_extract = r.take_u16().await?;
    // The version is stored in the last 8 bits of the field,
    // if the version is larger than 63 it's likely a false positive.
    if (version_to_extract & 0xff) > 63 {
        return Ok(None);
    }

    let _general_purpose_flags = r.take_u16().await?;
    let compression_method_id = r.take_u16().await?;
    let Some(compression_method) = CompressionMethod::from_id(compression_method_id) else {
        return Ok(None);
    };
    assert_eq!(
        compression_method,
        CompressionMethod::Deflated,
        "only DEFLATE compression is supported"
    );

    let _last_modification_time = r.take_u16().await?;
    let _last_modification_date = r.take_u16().await?;

    let _crc32 = r.take_u32().await?;
    let _compressed_size = r.take_u32().await?;
    let _uncompressed_size = r.take_u32().await?;

    let filename_length = r.take_u16().await?;
    let extra_field_length = r.take_u16().await?;

    let _filename = r.skip_bytes(filename_length as usize).await?;
    let _extra_field = r.skip_bytes(extra_field_length as usize).await?;

    Ok(Some(()))
}

/// Find the EOCD header.
/// Currently only checks the last 256 bytes of the file,
/// so if the EOCD is larger than 256 bytes it won't be found.
// TODO: implement handling of eocd larger than 256 bytes
async fn request_eocd(client: &Client, url: &Url, filesize: usize) -> Result<Option<Eocd>> {
    const CHUNK_SIZE: usize = 256;

    let from = filesize - CHUNK_SIZE;
    let to = filesize - 1;

    let resp = client
        .get(url.clone())
        .header("Range", format!("bytes={from}-{to}"))
        .send()
        .await?
        .error_for_status()?;

    let mut reader = Parser::new(resp.bytes_stream());

    let mut buf = [0u8; 4];
    let mut byte_offset = 0;

    while let Some(value) = reader.next().await? {
        buf[0] = value;
        buf.rotate_left(1);

        if buf == *b"PK\x05\x06" {
            if let MaybeEocd32::Eocd32(value) =
                read_eocd32(&mut reader, from + byte_offset, filesize).await?
            {
                return Ok(Some(value.into()));
            }
        } else if buf == *b"PK\x06\x06" {
            if let Some(value) = read_eocd64(&mut reader, from + byte_offset).await? {
                return Ok(Some(value.into()));
            }
        }

        byte_offset += 1;
    }

    Ok(None)
}

/// Read a EOCD32 record.
/// The given reader should return bytes right after the magic number `PK\x05\x06`.
async fn read_eocd32<S: ParserStream>(
    r: &mut Parser<S>,
    offset: usize,
    filesize: usize,
) -> Result<MaybeEocd32> {
    let this_disk_number = r.take_u16().await?;
    if this_disk_number > 256 && this_disk_number != 0xff {
        return Ok(MaybeEocd32::FalsePositive);
    }
    let cd_disk = r.take_u16().await?;
    if cd_disk > 256 && cd_disk != 0xff {
        return Ok(MaybeEocd32::FalsePositive);
    }
    let cd_records_on_disk = r.take_u16().await?;
    let cd_records_total = r.take_u16().await?;
    let cd_size = r.take_u32().await?;
    if cd_size as usize > filesize {
        return Ok(MaybeEocd32::FalsePositive);
    }
    let cd_offset = r.take_u32().await?;
    if cd_offset as usize > filesize {
        return Ok(MaybeEocd32::FalsePositive);
    }

    if this_disk_number == 0xff
        && cd_disk == 0xff
        && cd_records_on_disk == 0xff
        && cd_records_total == 0xff
        && cd_size == 0xffff
        && cd_offset == 0xffff
    {
        return Ok(MaybeEocd32::Zip64);
    }

    let comment_len = r.take_u16().await?;
    let _comment = r.skip_bytes(comment_len as usize).await?;

    Ok(MaybeEocd32::Eocd32(Eocd32 {
        this_disk_number,
        cd_disk,
        cd_records_on_disk,
        cd_records_total,
        cd_size,
        cd_offset,
        offset,
    }))
}

enum MaybeEocd32 {
    FalsePositive,
    Zip64,
    Eocd32(Eocd32),
}

/// Read a EOCD64 or None if it is a false positive.
/// The given reader should return bytes right after the magic number `PK\x06\x06`.
async fn read_eocd64<S: ParserStream>(r: &mut Parser<S>, offset: usize) -> Result<Option<Eocd64>> {
    let _size = r.take_u64().await?;
    let version_made_by = r.take_u16().await?;
    let version_to_extract = r.take_u16().await?;
    // The version is stored in the last 8 bits of the field,
    // if the version is larger than 63 it's likely a false positive.
    if (version_made_by & 0xff) > 63 || (version_to_extract & 0xff) > 63 {
        return Ok(None);
    }
    let this_disk_number = r.take_u32().await?;
    let cd_disk = r.take_u32().await?;
    let cd_records_on_disk = r.take_u64().await?;
    let cd_records_total = r.take_u64().await?;
    let cd_size = r.take_u64().await?;
    let cd_offset = r.take_u64().await?;
    //let _comment = r.read_bytes

    Ok(Some(Eocd64 {
        this_disk_number,
        cd_disk,
        cd_records_on_disk,
        cd_records_total,
        cd_size,
        cd_offset,
        offset,
    }))
}

/// Make a HEAD request and retrive the Content-Length header.
///
/// # Errors
/// If the Content-Length is not present or malformed.
async fn request_content_length(client: &Client, url: &Url) -> Result<usize> {
    let resp = client.head(url.clone()).send().await?;

    let Some(value) = resp.headers().get("content-length") else {
        return Err(Error::ContentLengthMissing);
    };

    let value = match value.to_str() {
        Ok(value) => value,
        Err(e) => return Err(Error::ContentLengthInvalidAscii(e)),
    };

    match value.parse() {
        Ok(value) => Ok(value),
        Err(e) => Err(Error::ContentLengthParse(e)),
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    TryChunks(#[from] TryChunksError<Bytes, reqwest::Error>),
    #[error("missing content-length header")]
    ContentLengthMissing,
    #[error("content-length is not valid ascii")]
    ContentLengthInvalidAscii(ToStrError),
    #[error("couldn't parse content-length as a number: {0}")]
    ContentLengthParse(ParseIntError),
    #[error("unexpected eof")]
    UnexpectedEof,
    #[error("eocd not found")]
    EocdNotFound,
    #[error("cd: file not found")]
    CdFileNotFound,
    #[error("malformed file header")]
    MalformedFileHeader,
}
