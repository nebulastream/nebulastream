#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <Core/TupleBuffer.hpp>
#include <fstream>
#include <string>
#include <SourceSink/DataSource.hpp>

namespace iotdb {

/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
 public:
  /**
   * @brief constructor of CSV source
   * @param schema of the source
   * @param path to the csv file
   * @param delimiter inside the file, default ","
   */
  CSVSource(const Schema& schema, const std::string& file_path,
            const std::string& delimiter = ",");

  /**
   * @brief override the receiveData method for the csv source
   * @return returns a buffer if available
   */
  TupleBufferPtr receiveData();

  /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
  const std::string toString() const;

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
  std::string delimiter;
};
}  // namespace iotdb

#endif /* INCLUDE_CSVSOURCE_H_ */
