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
#include <Util/BlockingQueue.hpp>

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
   * @brief print statistics about the buffer manager interaction to the info log
   */
  void printStatistics();

  /**
   * @brief reset all buffer pools
   */
  void reset();

  /**
   * FIXED SIZE BUFFER METHODS
   */

  /**
   * @brief get a free buffer of default size
   * @return Pointer to free buffer
   */
  TupleBufferPtr getFixedSizeBuffer();

  /**
   * @brief get a free buffer of default size with timeout
   * @param timeout in ms
   * @return Pointer to free buffer
   */
  TupleBufferPtr getFixedSizeBufferWithTimeout(size_t timeout_ms);

  /**
   * @brief release a given buffer such that it can be reused
   * @param Pointer to the buffer to be released
   * @return bool indicating if buffer was released, if false buffer was not present
   */
  bool releaseFixedSizeBuffer(const TupleBufferPtr tupleBuffer);

  /**
   * @brief delete and create n new buffers of currentBufferSize
   * CAUTION: this deletes all existing buffers
   * @param number of new buffers
   */
  void resizeFixedBufferCnt(size_t newBufferCnt);

  /**
   * @brief delete and re-create all buffers of new size (same buffer cnt as before)
   * CAUTION: this deletes all existing buffers
   * @param new size of buffer new buffers
   */
  void resizeFixedBufferSize(size_t newBufferSizeInByte);

  /**
   * @brief return the size of one buffer in bytes
   * @return size of a buffer in bytesfor (auto& entry : fixSizeBufferPool) {
   //TODO: we have to make sure that no buffer is currently used
   */
  size_t getFixedBufferSize();

  /**
   --------------------
   * VAR SIZE BUFFER METHODS
   * ----------------------
   */

  /**
   * @brief return the total number of buffer used in the buffer manager
   * @return number of buffer
   */
  size_t getNumberOfFixedBuffers();

  /**
   * @brief return the number of buffer free in the buffer manager
   * @return number free of buffer
   */
  size_t getNumberOfFreeFixedBuffers();

  /**
   * @brief release a given buffer such that it can be reused
   * @param Pointer to the buffer to be released
   * @return bool indicating if buffer was released, if false buffer was not present
   */
  bool releaseVarSizedBuffer(const TupleBufferPtr tupleBuffer);

  /**
   * @brief return the total number of var buffer used in the buffer manager
   * @return number of buffer
   */
  size_t getNumberOfVarBuffers();

  /**
   * @brief return the number of var buffer free in the buffer manager
   * @return number free of buffer
   */
  size_t getNumberOfFreeVarBuffers();

  /**
   * @brief create a new buffer of size variable size
   * @return Pointer to free buffer
   */
  TupleBufferPtr createVarSizeBuffer(size_t bufferSizeInByte);

  /**
   * @brief get a variable buffer with size larger than bufferSizeInByte
   * @note if no such buffer exists, a nullptr is returned
   * @return Pointer to free buffer
   */
  TupleBufferPtr getVarSizeBufferLargerThan(size_t bufferSizeInByte);

 private:
  /* implement singleton semantics: no construction,
   * copying or destruction of Buffer Manager objects
   * outside of the class
   * The default settings are:
   *  - bufferCnt = 1000
   *  - bufferSizeInByte = 4KB
   * */
  BufferManager();
//  BufferManager(const BufferManager&);
//  BufferManager& operator=(const BufferManager&)

  ~BufferManager();

  /**
   * @brief removes all buffers from the pool
   */
  void clearFixBufferPool();

  void clearVarBufferPool();

  /**
   * @brief add buffer with default size
   */
  void addOneBufferWithFixedSize();

  /**
   * @brief add buffer with default size
   */
  TupleBufferPtr addOneBufferWithVarSize(size_t bufferSizeInByte);

  //Map containing Tuple Pointer and if it is currently used
  BlockingQueue<TupleBufferPtr>* fixedSizeBufferPool;
  std::map<TupleBufferPtr, /**used*/std::atomic<bool>> varSizeBufferPool;

  size_t currentBufferSize;
  size_t numberOfFreeVarSizeBuffers;

  std::mutex changeBufferMutex;
  std::mutex resizeMutex;

  //statistics
  size_t noFreeBuffer;
  size_t providedBuffer;
  size_t releasedBuffer;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
}
#endif
