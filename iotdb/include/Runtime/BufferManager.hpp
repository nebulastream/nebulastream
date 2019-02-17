#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <Core/DataTypes.hpp>
#include <Core/TupleBuffer.hpp>

namespace iotdb {

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

    std::map<uint64_t, std::vector<TupleBufferPtr> > buffer_pool;

    std::mutex mutex;
    std::condition_variable cv;
    uint32_t capacity;
  };

  typedef std::shared_ptr<BufferManager> BufferManagerPtr;
}

#endif
