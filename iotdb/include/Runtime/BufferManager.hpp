#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <Core/DataTypes.hpp>
#include <Core/TupleBuffer.hpp>

namespace iotdb {

#define USE_LOCK_IMPL 0
#define USE_CAS_IMPL 1

  class TupleBuffer;
  typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

  class BufferManager{
  public:

    TupleBufferPtr getBuffer(const uint64_t size=4096);
    void releaseBuffer(TupleBufferPtr tupleBuffer);

    static BufferManager& instance();
    void unblockThreads() { cv.notify_all(); }

    uint32_t getCapacity() const { return capacity; }

  private:
    BufferManager();
    BufferManager(const BufferManager&);
    BufferManager& operator= (const BufferManager&);
    ~BufferManager();

    TupleBufferPtr getBufferByLock(const uint64_t size);
    TupleBufferPtr getBufferByCAS(const uint64_t size);

    void releaseBufferByLock(TupleBufferPtr tupleBuffer);
    void releaseBufferByCAS(TupleBufferPtr tupleBuffer);

    class BufferEntry {
    public:
      TupleBufferPtr buffer;
      std::atomic<int> used;
      BufferEntry () : buffer(nullptr), used(0) {}
      BufferEntry (TupleBufferPtr buf) : buffer(buf), used(0) {}
    };
    // buffer_size, a list of available buffers

#if USE_LOCK_IMPL
    // 1. Lock Version
    std::map<uint64_t, std::vector<TupleBufferPtr> > buffer_pool;
#elif USE_CAS_IMPL
    // 2. CAS Version
    std::map<uint64_t, std::vector<BufferEntry> > buffer_pool;
#endif
    std::mutex mutex;
    std::condition_variable cv;
    uint32_t capacity;

  };

  typedef std::shared_ptr<BufferManager> BufferManagerPtr;
}

#endif
