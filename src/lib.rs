#![allow(clippy::let_unit_value)]
use std::{io, num::ParseIntError};

use bytes::{Buf, Bytes};
use futures::stream::TryChunksError;
use reqwest::{Client, Url, header::ToStrError};

use crate::{
    parser::{Parser, ParserStream},
    structs::{Cdfh, CompressionMethod, Eocd, Eocd32, Eocd64},
};

mod parser;
mod structs;

/// Extract a single file from a zip.
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
    let reader = read_file_at_cdfh(client, url, &cdfh).await?;
    println!("Got File Header in {:?}", std::time::Instant::now() - start);

    Ok(reader)
}

/// Read the file contents at the file header offset in the given CDFH.
async fn read_file_at_cdfh(
    client: &Client,
    url: &Url,
    cdfh: &Cdfh,
) -> Result<impl io::Read + use<>> {
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

    Ok(reader)
}

/// Find a file inside the central directory.
async fn find_in_cd(
    client: &Client,
    url: &Url,
    eocd: &Eocd,
    filename: &str,
) -> Result<Option<Cdfh>> {
    let mut parser = request_cd(client, url, eocd).await?;

    while let Some(cdfh) = parser.next().await? {
        if cdfh.filename == filename {
            return Ok(Some(cdfh));
        }
    }

    Ok(None)
}

/// Make a range request for the Central Directory and pass the response data to `CdParser`.
async fn request_cd(
    client: &Client,
    url: &Url,
    eocd: &Eocd,
) -> Result<CdParser<impl ParserStream>> {
    let range = format!("bytes={}-{}", eocd.cd_offset, eocd.cd_offset + eocd.cd_size);

    let resp = client
        .get(url.clone())
        .header("Range", range)
        .send()
        .await?;

    let parser = Parser::new(resp.bytes_stream());

    Ok(CdParser::new(parser, eocd.offset))
}

/// Parses the content directory and provides an API similar to Iterator but for Content Directory File Headers.
struct CdParser<S: ParserStream> {
    parser: Parser<S>,
    maximum_allowed_offset: usize,
}

impl<S: ParserStream> CdParser<S> {
    fn new(parser: Parser<S>, maximum_allowed_offset: usize) -> Self {
        Self {
            parser,
            maximum_allowed_offset,
        }
    }

    async fn next(&mut self) -> Result<Option<Cdfh>> {
        let mut peek_buf: [u8; 4] = [0; 4];

        while let Some(value) = self.parser.next().await? {
            peek_buf.rotate_left(1);
            peek_buf[peek_buf.len() - 1] = value;

            // Is it a CDFH?
            if peek_buf == *b"PK\x01\x02" {
                let cdfh = match read_cdfh(&mut self.parser, self.maximum_allowed_offset).await {
                    Ok(None) | Err(Error::UnexpectedEof) => continue,
                    Ok(Some(cdfh)) => cdfh,
                    Err(e) => return Err(e),
                };

                return Ok(Some(cdfh));
            }
        }

        Ok(None)
    }
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
        } else if buf == *b"PK\x06\x06"
            && let Some(value) = read_eocd64(&mut reader, from + byte_offset).await?
        {
            return Ok(Some(value.into()));
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
