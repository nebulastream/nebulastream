// BufferIterator: iterates over the data segments of a TupleBuffer and its children.
//
// Mirrors the C++ BufferIterator (SinksParsing/BufferIterator.hpp). Sinks receive
// a BufferIterator instead of a raw TupleBuffer, so they don't need to know about
// buffer internals, child buffers, or the numberOfTuples-as-byte-count convention.
//
// Each call to `next_segment()` returns a byte slice of valid data:
//   - First: the main buffer's valid region (numberOfTuples bytes)
//   - Then: each child buffer's valid region (child.numberOfTuples bytes)
//   - Finally: None when all segments are exhausted

use crate::buffer::TupleBufferHandle;

/// Iterates over the data segments of a buffer and its children.
///
/// Sink authors use this to write output without knowing about TupleBuffer
/// internals. Each segment is a `&[u8]` of valid data bytes.
///
/// # Example
/// ```ignore
/// while let Some(segment) = iter.next_segment() {
///     file.write_all(segment).await?;
/// }
/// ```
pub struct BufferIterator {
    buffer: TupleBufferHandle,
    /// Current child buffer being yielded (loaded lazily).
    current_child: Option<TupleBufferHandle>,
    /// Next index to yield: 0 = main buffer, 1.. = child buffers.
    index: u32,
    /// Total segments: 1 (main) + number of child buffers.
    total: u32,
}

unsafe impl Send for BufferIterator {}

impl BufferIterator {
    /// Create a new iterator over the given buffer and its children.
    pub fn new(buffer: TupleBufferHandle) -> Self {
        let num_children = buffer.num_child_buffers();
        Self {
            buffer,
            current_child: None,
            index: 0,
            total: 1 + num_children,
        }
    }

    /// Return the next data segment, or `None` when all segments are exhausted.
    ///
    /// The first segment is the main buffer's valid data (numberOfTuples bytes).
    /// Subsequent segments are child buffers' valid data.
    pub fn next_segment(&mut self) -> Option<&[u8]> {
        if self.index >= self.total {
            return None;
        }

        if self.index == 0 {
            self.index += 1;
            let content_len = self.buffer.number_of_tuples() as usize;
            return Some(&self.buffer.as_slice()[..content_len]);
        }

        // Load child buffer (index - 1 because child indices are 0-based)
        let child_index = self.index - 1;
        self.index += 1;
        let child = self.buffer.load_child_buffer(child_index);
        let content_len = child.number_of_tuples() as usize;
        self.current_child = Some(child);
        let child_ref = self.current_child.as_ref().unwrap();
        Some(&child_ref.as_slice()[..content_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_yields_main_buffer_segment() {
        let mut buf = TupleBufferHandle::new_test();
        buf.as_mut_slice()[..4].copy_from_slice(&42u32.to_le_bytes());
        buf.set_number_of_tuples(4);

        let mut iter = BufferIterator::new(buf);

        let segment = iter.next_segment().expect("should yield main buffer");
        assert_eq!(segment.len(), 4);
        assert_eq!(u32::from_le_bytes(segment[..4].try_into().unwrap()), 42);

        assert!(iter.next_segment().is_none(), "no child buffers in test");
    }

    #[test]
    fn iterator_yields_nothing_for_empty_buffer() {
        let buf = TupleBufferHandle::new_test();
        // number_of_tuples = 0 (default)

        let mut iter = BufferIterator::new(buf);
        let segment = iter.next_segment().expect("should yield main buffer");
        assert_eq!(segment.len(), 0, "empty buffer yields zero-length slice");
        assert!(iter.next_segment().is_none());
    }

    #[test]
    fn iterator_is_send() {
        fn _assert_send<T: Send>() {}
        _assert_send::<BufferIterator>();
    }
}
