use std::mem::{self, MaybeUninit};

use super::{Error, Result};
use bytes::Bytes;
use futures::{TryStream, TryStreamExt};

pub struct Parser<S: ParserStream> {
    stream: S,
    bytes: Bytes,
    is_eof: bool,
}

impl<S: ParserStream> Parser<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            bytes: Bytes::new(),
            is_eof: false,
        }
    }
}

impl<S: ParserStream> Parser<S> {
    pub async fn skip_bytes(&mut self, length: usize) -> Result<()> {
        self.take_bytes(length).await.map(|_| ())
    }

    pub async fn take_bytes(&mut self, length: usize) -> Result<Bytes> {
        self.request_atleast(length).await?;
        if self.bytes.len() < length {
            return Err(Error::UnexpectedEof);
        }
        Ok(self.bytes.split_to(length))
    }

    pub async fn take_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take_fixed_slice().await?))
    }

    pub async fn take_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take_fixed_slice().await?))
    }

    pub async fn take_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take_fixed_slice().await?))
    }

    pub async fn take_fixed_slice<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.request_atleast(N).await?;
        if self.bytes.len() < N {
            return Err(Error::UnexpectedEof);
        }

        let bytes = self.bytes.split_to(N);
        let mut slice: MaybeUninit<[u8; N]> = MaybeUninit::uninit();
        debug_assert_eq!(bytes.len(), N);

        for i in 0..N {
            unsafe { slice.assume_init_mut()[i] = bytes[i] }
        }

        Ok(unsafe { slice.assume_init() })
    }

    pub async fn next(&mut self) -> Result<Option<u8>> {
        self.request_atleast(1).await?;
        if self.bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.bytes.split_to(1)[0]))
    }

    /// If there isn't enough bytes, ask for more.
    pub async fn request_atleast(&mut self, atleast: usize) -> Result<()> {
        if self.bytes.len() < atleast && !self.is_eof {
            match self.stream.try_next().await? {
                Some(next) => {
                    self.bytes = [mem::take(&mut self.bytes), next].concat().into();
                }
                None => self.is_eof = true,
            }
        }
        Ok(())
    }
}

pub trait ParserStream: TryStream<Ok = Bytes, Error = reqwest::Error> + Unpin {}
impl<T: TryStream<Ok = Bytes, Error = reqwest::Error> + Unpin> ParserStream for T {}

/*
pub trait ReadExt
where
    Self: AsyncRead + Unpin,
{
    async fn read_bytes(&mut self, length: usize) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0; length];
        self.read_exact(bytes.as_mut_slice()).await?;
        Ok(bytes)
    }

    async fn skip_bytes(&mut self, length: usize) -> io::Result<()> {
        self.read_bytes(length).await.map(|_| ())
    }

    async fn read_fixed_slice<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut buf: MaybeUninit<[u8; N]> = MaybeUninit::uninit();
        self.read_exact(unsafe { buf.assume_init_mut() }).await?;
        Ok(unsafe { buf.assume_init() })
    }

    async fn read_u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.read_fixed_slice().await?))
    }

    async fn read_u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.read_fixed_slice().await?))
    }

    async fn read_u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.read_fixed_slice().await?))
    }
}

impl<T: AsyncRead + Unpin> ReadExt for T {}
 */
