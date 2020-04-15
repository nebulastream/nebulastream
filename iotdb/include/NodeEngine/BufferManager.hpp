#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>
#include <optional>
//#include "API/Types/DataTypes.hpp"

namespace NES {

class MemorySegment;
class TupleBuffer;

class BufferManager {
    friend class TupleBuffer;
    friend class MemorySegment;
  private:
    struct UnpooledBufferHolder {
        uint32_t size;
        MemorySegment* segment;

        friend bool operator<(const UnpooledBufferHolder& lhs, const UnpooledBufferHolder& rhs) {
            return lhs.size < rhs.size;
        }
    };
  public:
    BufferManager();

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager();

    void configure(size_t bufferSize, size_t numOfBuffers);

    TupleBuffer getBufferBlocking();
    std::optional<TupleBuffer> getBufferNoBlocking();
    std::optional<TupleBuffer> getBufferTimeout(std::chrono::milliseconds timeout_ms);
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize);


    size_t getBufferSize() const;
    size_t getNumOfPooledBuffers() const;
    size_t getNumOfUnpooledBuffers();
    size_t getAvailableBuffers();

    static BufferManager& instance();

    void printStatistics();

    void requestShutdown();

  private:
    void recyclePooledBuffer(MemorySegment* buffer);
    void recycleUnpooledBuffer(MemorySegment* buffer);

  private:
    std::vector<MemorySegment> allBuffers;
    std::vector<MemorySegment*> availableBuffers;
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
