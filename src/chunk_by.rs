/// Takes a size and chunk count and divides the size evenly between all the chunks,
/// outputs (from, to) inclusive byte ranges.
pub struct EvenChunkBy {
    chunk_size: u64,
    remainder: u64,
    from: u64,
    chunks: u64,
}

impl EvenChunkBy {
    pub fn new(size: u64, chunks: u64) -> Self {
        let chunk_size = size / chunks;

        Self {
            chunk_size,
            remainder: size % chunk_size,
            from: 0,
            chunks,
        }
    }
}

impl Iterator for EvenChunkBy {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunks <= 0 {
            return None;
        }
        self.chunks -= 1;

        let remainder = if self.remainder <= 0 {
            0
        } else {
            self.remainder -= 1;
            1
        };

        let size = self.chunk_size + remainder;

        let from = self.from;
        let to = self.from + size - 1;
        self.from = to + 1;

        Some((from, to))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn iterate_chunks() {
        let mut chunks = EvenChunkBy::new(29, 5);
        assert_eq!(chunks.next(), Some((0, 5)));
        assert_eq!(chunks.next(), Some((6, 11)));
        assert_eq!(chunks.next(), Some((12, 17)));
        assert_eq!(chunks.next(), Some((18, 23)));
        assert_eq!(chunks.next(), Some((24, 28)));
        assert_eq!(chunks.next(), None);
    }
}
