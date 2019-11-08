#ifndef FILEOUTPUTSINK_HPP
#define FILEOUTPUTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <SourceSink/DataSink.hpp>

namespace iotdb {

/**
 * @brief this class implements the File output sink
 */
class FileOutputSink : public DataSink {

 public:
/**
 * @brief default constructor that creates an empty file sink
 */
  FileOutputSink();

  /**
   * @brief constructor that creates an empty file sink using a schema
   * @param schema of the print sink
   */
  FileOutputSink(const Schema& schema);

  /**
   * @brief method to override virtual setup function
   * @Note currently the method does nothing
   */
  void setup() override;

  /**
   * @brief method to override virtual shutdown function
   * @Note currently the method does nothing
   */
  void shutdown() override;

  /**
   * @brief method to override virtual write data function
   * @Note currently not implemented
   * @param tuple buffer pointer to be filled
   * @return bool indicating write success
   */
  bool writeData(const TupleBufferPtr input_buffer);


  /**
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
  const std::string toString() const override;

 private:
};
}  // namespace iotdb

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::FileOutputSink)

#endif // FILEOUTPUTSINK_HPP
