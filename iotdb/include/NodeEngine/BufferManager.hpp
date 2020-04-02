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

class TupleBuffer;

#ifndef USE_NEW_BUFFER_MANAGEMENT
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;
#else
#endif
#ifndef USE_NEW_BUFFER_MANAGEMENT
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
  static BufferManager &instance();

  /**
   * @brief add buffer with default size
   */
  void addOneBufferWithDefaultSize();

  /**
   * @brief remove a particular buffer from the buffer pool
   * @param Pointer to buffer to be deleted
   * @return true if buffer was deleted, false if buffer was not present
   */
  bool removeBuffer(TupleBufferPtr tuple_buffer);

  /**
   * @brief get a free buffer of default size
   * @return Pointer to free buffer
   */
  TupleBufferPtr getBuffer();

  /**
   * @brief release a given buffer such that it can be reused
   * @param Pointer to the buffer to be released
   * @return bool indicating if buffer was released, if false buffer was not present
   */
  bool releaseBuffer(const TupleBufferPtr tuple_buffer);

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
  size_t getBufferSize();

  /**
   * @brief print statistics about the buffer manager interaction to the info log
   */
  void printStatistics();

  /**
   * @brief delete and create n new buffers
   * CAUTION: this deletes all existing buffers
   * @param number of new buffers
   */
  void setNumberOfBuffers(size_t n);

  /**
     * @brief delete and recreate all buffers with the new buffer size,
     * buffer count remains the same
     * CAUTION: this deletes all existing buffers
     * @param size of new buffers
     */
  void setBufferSize(size_t size);

 private:
  /* implement singleton semantics: no construction,
   * copying or destruction of Buffer Manager objects
   * outside of the class
   * The default settings are:
   *  - bufferCnt = 1000
   *  - bufferSizeInByte = 4KB
   * */
  BufferManager();
  BufferManager(const BufferManager &);
  BufferManager &operator=(const BufferManager &);
  ~BufferManager();

  //Map containing Tuple Pointer and if it is currently used
  std::map<TupleBufferPtr, std::atomic<bool>> buffer_pool;

  size_t bufferSizeInByte;

  std::mutex mutex;

  //statistics
  size_t noFreeBuffer;
  size_t providedBuffer;
  size_t releasedBuffer;
  size_t maxNumberOfRetry;
};

typedef std::shared_ptr<BufferManager> BufferManagerPtr;
#else

class MemorySegment;
class TupleBuffer;

class BufferManager {
    friend class TupleBuffer;
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

#endif

}
#endif
