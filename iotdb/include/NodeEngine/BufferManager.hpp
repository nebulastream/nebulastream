#ifndef _BUFFER_MANAGER_H
#define _BUFFER_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "API/Types/DataTypes.hpp"
#include "TupleBuffer.hpp"

namespace NES {

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

/**
 * @brief the buffer manager serves as the general maintainer of
 * Resource allocation inside the engine.
 * Default buffers are initialized during construction using addBuffer()
 * @Limitations:
 *    - Only one sized buffers can be handled
 *    - Each buffer can only be provided once, no buffer sharing (unclear what happens when pointer is passed around)
 *    - Current Locking scheme is too restrictive
 */
class BufferManager {

 public:
  /**
   * @brief Singleton implementation of Buffer Manager
   */
  static BufferManager& instance();

  /**
   * @brief remove a particular buffer from the buffer pool
   * @param Pointer to buffer to be deleted
   * @return true if buffer was deleted, false if buffer was not present
   */
  bool removeBuffer(TupleBufferPtr tupleBuffer);

  /**
   * @brief get a free buffer of default size
   * @return Pointer to free buffer
   */
  TupleBufferPtr getFixSizeBuffer();

  /**
   * @brief create a new buffer of size varible size
   * @return Pointer to free buffer
   */
  TupleBufferPtr createVarSizeBuffer(size_t bufferSizeInByte);

  /**
   * @brief release a given buffer such that it can be reused
   * @param Pointer to the buffer to be released
   * @return bool indicating if buffer was released, if false buffer was not present
   */
  bool releaseBuffer(const TupleBufferPtr tupleBuffer);

  /**
   * @brief return the total number of buffer used in the buffer manager
   * @return number of buffer
   */
  size_t getNumberOfBuffers();

  /**
   * @brief return the number of buffer free in the buffer manager
   * @return number free of buffer
   */
  size_t getNumberOfFreeBuffers();

  /**
   * @brief return the size of one buffer in bytes
   * @return size of a buffer in bytes
   */
  size_t getFixBufferSize();

  /**
   * @brief print statistics about the buffer manager interaction to the info log
   */
  void printStatistics();

  /**
   * @brief delete and create n new buffers of currentBufferSize
   * CAUTION: this deletes all existing buffers
   * @param number of new buffers
   */
  void resizeFixBufferCnt(size_t newBufferCnt);

  /**
   * @brief delete and re-create all buffers of new size (same buffer cnt as before)
   * CAUTION: this deletes all existing buffers
   * @param new size of buffer new buffers
   */
  void resizeFixBufferSize(size_t newBufferSizeInByte);

 private:
  /* implement singleton semantics: no construction,
   * copying or destruction of Buffer Manager objects
   * outside of the class
   * The default settings are:
   *  - bufferCnt = 1000
   *  - bufferSizeInByte = 4KB
   * */
  BufferManager();
  BufferManager(const BufferManager&);
  BufferManager& operator=(const BufferManager&);
  ~BufferManager();

  /**
   * @brief removes all buffers from the pool
   */
  void clearFixBufferPool();

  /**
   * @brief add buffer with default size
   */
  void addOneBufferWithFixSize();

  /**
   * @brief add buffer with default size
   */
  void addOneBufferWithVarSize(size_t bufferSizeInByte);

  //Map containing Tuple Pointer and if it is currently used
  std::map<TupleBufferPtr, /**used*/ std::atomic<bool>> fixSizeBufferPool;

  std::map<TupleBufferPtr, /**used*/ std::atomic<bool>> varSizeBufferPool;

  size_t currentBufferSize;

  std::mutex mutex;

  //statistics
  size_t noFreeBuffer;
  size_t providedBuffer;
  size_t releasedBuffer;
  size_t maxNumberOfRetry;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
}
#endif
