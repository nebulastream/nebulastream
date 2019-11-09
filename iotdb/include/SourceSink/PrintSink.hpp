#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <SourceSink/DataSink.hpp>

namespace iotdb {

/**
 * @brief this class provides a print sink
 */
class PrintSink : public DataSink {
 public:

  /**
   * @brief Default constructor
   * @Note the default output will be written to cout
   */
  PrintSink(std::ostream& pOutputStream = std::cout);

  /**
     * @brief Default constructor
     * @Note the default output will be written to cout
     * @param schema of the written buffer tuples
     */
  PrintSink(const Schema& pSchema, std::ostream& pOutputStream = std::cout);

  /**
   * @brief destructor
   * @Note this is required by some tests
   * TODO: find out why this is required
   */
  ~PrintSink();

  /**
   * @brief setup method for print sink
   * @Note required due to derivation but does nothing
   */
  void setup() override;

  /**
   * @brief shutdown method for print sink
   * @Note required due to derivation but does nothing
   */
  void shutdown() override;

/**
 * @brief method to write the content of a tuple buffer to output console
 * @param tuple buffer to write
 * @return bool indicating success of the write
 */
  bool writeData(const TupleBufferPtr input_buffer);

  /**
   * @brief override the toString method for the print sink
   * @return returns string describing the print sink
   */
  const std::string toString() const override;

 protected:
  friend class boost::serialization::access;

  template<class Archive> void serialize(Archive& ar,
                                         const unsigned int version) {
    ar & boost::serialization::base_object<DataSink>(*this);
  }

 private:
  std::ostream& outputStream;
};

}  // namespace iotdb

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::PrintSink)

#endif // PRINTSINK_HPP
