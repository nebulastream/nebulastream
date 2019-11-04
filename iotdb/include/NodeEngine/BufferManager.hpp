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

/**
 * @brief the buffer manager serves as the general maintainer of
 * Resource allocation inside the engine
 */
class BufferManager {

 public:
  static BufferManager &instance();


  void addBuffer();
  void removeBuffer(TupleBufferPtr tuple_buffer);
  TupleBufferPtr getBuffer();
  void releaseBuffer(const TupleBufferPtr tuple_buffer);
  void releaseBuffer(const TupleBuffer* tuple_buffer);
  size_t getNumberOfBuffers();
  size_t getNumberOfFreeBuffers();
  size_t getBufferSize();
  void printStatistics();

  void setNumberOfBuffers(size_t size);
  void setBufferSize(size_t size);

 private:
  /* implement singleton semantics: no construction,
   * copying or destruction of Buffer Manager objects
   * outside of the class */
  BufferManager();
  BufferManager(const BufferManager &);
  BufferManager &operator=(const BufferManager &);
  ~BufferManager();

  std::map<TupleBufferPtr, std::atomic<bool>> buffer_pool;  //make bool atomic
  size_t maxBufferCnt;
  size_t bufferSizeInByte;

  std::mutex mutex;

  //statistics
  size_t noFreeBuffer;
  size_t providedBuffer;
  size_t releasedBuffer;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
}
#endif
