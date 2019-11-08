#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include <Core/TupleBuffer.hpp>
#include <fstream>
#include "../SourceSink/DataSource.hpp"

namespace iotdb {

/**
 * @brief this class provides a binary file as source
 */
class BinarySource : public DataSource {
 public:

  /**
   * @brief constructor for binary source
   * @param schema of the data source
   * @param file path
   */
  BinarySource(const Schema& schema, const std::string& file_path);

/**
 * @brief override the receiveData method for the binary source
 * @return returns a buffer if available
 */
  TupleBufferPtr receiveData() override;

  /**
   * @brief override the toString method for the binary source
   * @return returns string describing the binary source
   */
  const std::string toString() const override;

  /**
   *  @brief method to fill the buffer with tuples
   *  @param buffer to be filled
   */
  void fillBuffer(TupleBuffer&);

 private:
  std::ifstream input;
  const std::string file_path;

  int64_t file_size;
  size_t tuple_size;
};
}  // namespace iotdb

#endif /* INCLUDE_BINARYSOURCE_H_ */
