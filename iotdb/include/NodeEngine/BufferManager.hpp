#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <deque>
#include <vector>
#include <optional>
//#include "API/Types/DataTypes.hpp"

namespace NES {
namespace detail {
class MemorySegment;
}
class TupleBuffer;

class BufferManager {
    friend class TupleBuffer;
    friend class detail::MemorySegment;
  private:
    struct UnpooledBufferHolder {
        uint32_t size;
        detail::MemorySegment* segment;

        friend bool operator<(const UnpooledBufferHolder& lhs, const UnpooledBufferHolder& rhs) {
            return lhs.size < rhs.size;
        }
    };
  public:
    BufferManager();

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
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within timeout_ms.
     * @param timeout_ms the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferTimeout(std::chrono::milliseconds timeout_ms);

    /**
     * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error occurs.
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

    static BufferManager& instance();

    void printStatistics();

    /**
     * @brief Disables the buffer manager
     */
    void requestShutdown();

    bool isReady() const {
        return isConfigured && !shutdownRequested;
    }

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

    bool shutdownRequested;

};

}
#endif
