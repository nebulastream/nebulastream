#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_
#include <cstdint>
#include <memory>
#include <API/Schema.hpp>
namespace NES {

//forward declaration
class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

/**
 * @brief this class handles the abstraction of tuples in byte buffer
 * @limitations
 *    - Tuple buffer can only store fixed sized tuples
 *    - Tuple buffer can only same size tuples
 *    - The buffer itself will currently not be serialized/deserialized
 */
class TupleBuffer {
 public:
  /**
   * @brief constructor for the tuple buffer
   * @param pointer to allocated memory for this buffer
   * @param size of buffer in bytes
   * @param size of one tuple in the buffer
   * @param number of tuples inside the buffer
   */
  TupleBuffer(void* buffer, const size_t buffer_size_bytes,
              const uint32_t tupleSizeBytes, const uint32_t numTuples);

  /**
   * @brief destructor for tuple buffer
   */
  ~TupleBuffer();

  /**
   * @brief Overload of the = operator to copy a tuple buffer
   * @param other tuple buffer
   */
  TupleBuffer& operator=(const TupleBuffer&);

  TupleBuffer(const TupleBuffer& other) {
      *this = other;
  }

  TupleBuffer(TupleBuffer&&) = delete;
  TupleBuffer& operator=(TupleBuffer&&) = delete;

  /**
   * @brief Explicit copy method for tuple buffer using TupleBufferPtr
   * @param TupleBufferPtr of the buffer where we copy from into this buffer
   */
  void copyInto(const TupleBufferPtr);

  /**
   * @brief method to print the current statistics of the buffer
   */
  void print();

  /**
   * @brief getter for the number of tuples
   * @return number of tuples
   */
  size_t getNumberOfTuples();

  /**
   * @brief getter for the buffer size in bytes
   * @return buffer size in bytes
   */
  size_t getBufferSizeInBytes();

  /**
   * @brief getter for the size of one tuple
   * @return size of one tuple in bytes
   */
  size_t getTupleSizeInBytes();

  /**
   * @brief getter for the pointer to the buffer
   * @return void pointer to the buffer
   */
  void* getBuffer();

  /**
   * @brief setter for the number of tuples
   * @param number of tuples
   */
  void setNumberOfTuples(size_t number);

  /**
   * @brief setter for the buffer size in bytes
   * @param buffer size in bytes
   */
  void setBufferSizeInBytes(size_t size);

  /**
   * @brief setter for the tuple size in bytes
   * @param tuple size in bytes
   */
  void setTupleSizeInBytes(size_t size);

  /**
   * @brief setter/getter for the counter how often this buffer is use (to prevent early release)
   * @param tuple size in bytes
   */
  void setUseCnt(size_t size);
  size_t getUseCnt();

  /**
   * @brief decrement the counter by one and test if now zero
   * @return bool indicating success
   */
  bool decrementUseCntAndTestForZero();

  /**
   * @brief increment the counter by one
   * @return bool indicating succeess
   */
  void incrementUseCnt();

  /**
   * @brief this method creates a string from the content of a tuple buffer
   * @return string of the buffer content
   */
  std::string printTupleBuffer(SchemaPtr schema);

  /**
<<<<<<< HEAD
   * @brief revert the endianess of the tuple buffer
   * @schema of the buffer
   */
   void revertEndianness(SchemaPtr schema);

 private:
  /**
   * @brief default constructor for serialization with boost
   */
  TupleBuffer()
      :
      buffer(nullptr),
      bufferSizeInBytes(0),
      tupleSizeInBytes(0),
      numberOfTuples(0),
      useCnt(0) {
  }

  void* buffer;
  std::atomic<size_t> bufferSizeInBytes;
  std::atomic<size_t> tupleSizeInBytes;
  std::atomic<size_t>numberOfTuples;
  std::atomic<size_t> useCnt;
};

class SchemaTemp;
std::string toString(TupleBuffer& buffer, SchemaPtr schema);
std::string toString(TupleBuffer* buffer, SchemaPtr schema);

}  // namespace NES
#endif /* INCLUDE_TUPLEBUFFER_H_ */
