#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace NES {
namespace detail {
class MemorySegment;
}
class TupleBuffer;

/**
 * @brief The BufferManager is responsible for:
 * 1. Pooled Buffers: preallocated fixed-size buffers of memory that must be reference counted
 * 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly. They are also subject to reference
 * counting.
 *
 * The reference counting mechanism of the TupleBuffer is explained in TupleBuffer.hpp
 *
 * The BufferManager stores the pooled buffers as MemorySegment-s. When a component asks for a Pooled buffer,
 * then the BufferManager retrieves an available buffer (it blocks the calling thread, if no buffer is available).
 * It then hands out a TupleBuffer that is constructed through the pointer stored inside a MemorySegment.
 * This is necessary because the BufferManager must keep all buffers stored to ensure that when its
 * destructor is called, all buffers that it has ever created are deallocated. Note the BufferManager will check also
 * that no reference counter is non-zero and will throw a fatal exception, if a component hasnt returned every buffers.
 * This is necessary to avoid memory leaks.
 *
 * Unpooled buffers are either allocated on the spot or served via a previously allocated, unpooled buffer that has
 * been returned to the BufferManager by some component.
 *
 */
class BufferManager {
    friend class TupleBuffer;
    friend class detail::MemorySegment;

  private:
    class UnpooledBufferHolder {
      public:
        std::unique_ptr<detail::MemorySegment> segment;
        uint32_t size;
        bool free;

        UnpooledBufferHolder();

        UnpooledBufferHolder(uint32_t size);

        UnpooledBufferHolder(std::unique_ptr<detail::MemorySegment>&& mem, uint32_t size);

        void markFree();

        friend bool operator<(const UnpooledBufferHolder& lhs, const UnpooledBufferHolder& rhs) {
            return lhs.size < rhs.size;
        }
    };

  public:
    BufferManager();

    BufferManager(size_t bufferSize, size_t numOfBuffers);

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager();

    /**
     * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes.
     * This is a one shot call. A second invocation of this call will fail.
     * @param bufferSize
     * @param numOfBuffers
     */
    void configure(size_t bufferSize, size_t numOfBuffers);

    /**
     * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
     * @return a new buffer
     */
    TupleBuffer getBufferBlocking();

    /**
     * @brief Returns a new TupleBuffer wrapped in an optional or an invalid option if there is no buffer.
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferNoBlocking();

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeout_ms.
     * @param timeout_ms the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferTimeout(std::chrono::milliseconds timeout_ms);

    /**
     * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
     * occurs.
     * @param bufferSize
     * @return a new buffer
     */
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize);

    /**
     * @return Configured size of the buffers
     */
    size_t getBufferSize() const;

    /**
     * @return Number of total buffers in the pool
     */
    size_t getNumOfPooledBuffers() const;

    /**
     * @return number of unpooled buffers
     */
    size_t getNumOfUnpooledBuffers();

    /**
     * @return Number of available buffers in the pool
     */
    size_t getAvailableBuffers();

    void printStatistics();

    bool isReady() const { return isConfigured; }

  private:
    void recyclePooledBuffer(detail::MemorySegment* buffer);
    void recycleUnpooledBuffer(detail::MemorySegment* buffer);

  private:
    std::vector<detail::MemorySegment> allBuffers;
    std::deque<detail::MemorySegment*> availableBuffers;
    std::vector<UnpooledBufferHolder> unpooledBuffers;

    std::mutex availableBuffersMutex;
    std::condition_variable availableBuffersCvar;

    std::mutex unpooledBuffersMutex;

    uint32_t bufferSize;
    uint32_t numOfBuffers;

    std::atomic<bool> isConfigured;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;

}// namespace NES
#endif
