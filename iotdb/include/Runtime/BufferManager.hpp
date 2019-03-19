#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include <Core/DataTypes.hpp>
#include <Core/TupleBuffer.hpp>

namespace iotdb {

#define USE_LOCK_IMPL 0
#define USE_CAS_IMPL 1

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

class BufferManager {
public:
  void addBuffer();
  TupleBufferPtr getBuffer();
  size_t getNumberOfBuffers();
  size_t getNumberOfFreeBuffers();
  void releaseBuffer(TupleBufferPtr tuple_buffer);
  static BufferManager &instance();

private:
  BufferManager();
  BufferManager(const BufferManager &);
  BufferManager &operator=(const BufferManager &);
  ~BufferManager();

  std::map<TupleBufferPtr, bool> buffer_pool;//make bool atomic
  size_t maxBufferCnt;
  size_t bufferSizeInByte;

  std::mutex mutex;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
} // namespace iotdb
#endif
