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

#define USE_LOCK_IMPL 1
#define USE_CAS_IMPL 0

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

class BufferManager {
public:
  void addBuffers(const size_t number_of_buffers, const size_t buffer_size_bytes);
  TupleBuffer getBuffer(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes);
  void releaseBuffer(TupleBuffer tuple_buffer);

  static BufferManager &instance();
  void unblockThreads() { cv.notify_all(); }

private:
  BufferManager();
  BufferManager(const BufferManager &);
  BufferManager &operator=(const BufferManager &);
  ~BufferManager();

  // buffer_size, a list of available buffers
#if USE_LOCK_IMPL

  // 1. Lock Version
  void addBuffersByLock(const size_t number_of_buffers, const size_t buffer_size_bytes);
  TupleBuffer getBufferByLock(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes);
  void releaseBufferByLock(TupleBuffer tuple_buffer);
  std::map<size_t, std::vector<char *>> buffer_pool;

#elif USE_CAS_IMPL

  // 2. CAS Version
  class BufferEntry {
  public:
    char *buffer;
    std::atomic<int> used;
    BufferEntry() : buffer(nullptr), used(0) {}
    BufferEntry(char *buf) : buffer(buf), used(0) {}
  };

  void addBuffersByCAS(const size_t number_of_buffers, const size_t buffer_size_bytes);
  TupleBuffer getBufferByCAS(const uint32_t number_of_tuples, const uint32_t tuple_size_bytes);
  void releaseBufferByCAS(TupleBuffer tuple_buffer);
  std::map<size_t, std::vector<BufferEntry>> buffer_pool;
#endif

  std::mutex mutex;
  std::condition_variable cv;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
} // namespace iotdb

#endif
