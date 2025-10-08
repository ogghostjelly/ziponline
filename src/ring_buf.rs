use std::mem::{self, MaybeUninit};

/// Fixed-size ring buffer, like VecDeque but without heap allocations.
pub struct RingBuffer<const N: usize> {
    buf: [MaybeUninit<u8>; N],
    len: usize,
}

impl<const N: usize> RingBuffer<N> {
    /// Create a new ring buffer.
    pub fn new() -> Self {
        Self {
            buf: [const { MaybeUninit::uninit() }; N],
            len: 0,
        }
    }

    /// Clear the contents of the ring buffer, setting the length to 0.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Converts the ring buffer to a slice of any size between 0..N.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { mem::transmute(&self.buf[(N - self.len)..]) }
    }

    /// Push a new element onto the buffer and returns the removed element or None if there was no removed element.
    pub fn push(&mut self, value: u8) -> Option<u8> {
        self.buf.rotate_left(1);
        if self.len < N {
            self.buf[N - 1].write(value);
            self.len += 1;
            None
        } else {
            let elem = unsafe { self.buf[N - 1].assume_init_mut() };
            Some(mem::replace(elem, value))
        }
    }
}
