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

    TupleBufferPtr getBuffer(uint64_t size=4096);
    void releaseBuffer(TupleBufferPtr tupleBuffer);

    static BufferManager& instance();

  private:
    BufferManager();
    BufferManager(const BufferManager&);
    BufferManager& operator= (const BufferManager&);
    ~BufferManager();

    // class BufferEntry{
    // public:
    //   TupleBufferPtr buffer;
    //   bool used;
    //   BufferEntry (TupleBufferPtr buf) : buffer(buf), used(false) {
    //   }
    // };

    // std::map<uint64_t, std::shared_ptr<std::vector<TupleBufferPtr> > > buffer_pool;
    std::map<uint64_t, std::vector<TupleBufferPtr> > buffer_pool;
    // std::map<std::string, uint64_t > tester;

    std::mutex mutex;
    std::condition_variable cv;
    const uint32_t capacity = 10;
  };
}

#endif
