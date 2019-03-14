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
  void releaseBuffer(const TupleBufferPtr tuple_buffer);
  void releaseBuffer(const TupleBuffer* tuple_buffer);

  static BufferManager &instance();
  void unblockThreads() { cv.notify_all(); }

  void setNewBufferSize(size_t size);
private:
  BufferManager();
  BufferManager(const BufferManager &);
  BufferManager &operator=(const BufferManager &);
  ~BufferManager();

  std::map<TupleBufferPtr, bool> buffer_pool;//make bool atomic
  size_t maxBufferCnt;
  size_t bufferSizeInByte;

  std::mutex mutex;
  std::condition_variable cv;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
} // namespace iotdb
#endif
