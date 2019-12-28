#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <fstream>
#include <string>
#include <SourceSink/DataSource.hpp>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include "../NodeEngine/TupleBuffer.hpp"

namespace iotdb {

/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
 public:
  /**
   * @brief constructor of CSV sou1rce
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
   *  @brief method to fill the buffer with tuples
   *  @param buffer to be filled
   */
  void fillBuffer(TupleBufferPtr);

  /**
   * @brief override the toString method for the csv source
   * @return returns string describing the binary source
   */
  const std::string toString() const;
    SourceType getType() const override;
  private:
  CSVSource();

  std::string file_path;

//  int file_size;
  size_t tuple_size;
  std::string delimiter;

  /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
  friend class boost::serialization::access;
  template<class Archive> void serialize(Archive& ar,
                                         const unsigned int version) {
    ar & boost::serialization::base_object<DataSource>(*this);
    ar & file_path;
//    ar & file_size;
    ar & tuple_size;
    ar & delimiter;
    ar & generatedTuples;
    ar & generatedBuffers;
  }
};
}  // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::CSVSource)
#endif /* INCLUDE_CSVSOURCE_H_ */
