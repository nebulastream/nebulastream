#pragma once

#include <fstream>
#include <string>
#include <SourceSink/DataSource.hpp>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

namespace NES {
class TupleBuffer;
/**
 * @brief this class implement the CSV as an input source
 */
class SenseSource : public DataSource {
 public:
  /**
   * @brief constructor of sense sou1rce
   * @param schema of the source
   * @param udfs to apply
   */
  SenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher, const std::string& udfs);

  /**
   * @brief override the receiveData method for the source
   * @return returns a buffer if available
   */
  std::optional<TupleBuffer> receiveData() override;

  /**
   *  @brief method to fill the buffer with tuples
   *  @param buffer to be filled
   */
  void fillBuffer(TupleBuffer&);

  /**
   * @brief override the toString method for the csv source
   * @return returns string describing the binary source
   */
  const std::string toString() const;
  SourceType getType() const override;

  void setUdsf(std::string udsf);

  private:
  SenseSource();

  std::string udsf;
  /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
  friend class boost::serialization::access;
  template<class Archive> void serialize(Archive& ar,
                                         const unsigned int version) {
    ar & boost::serialization::base_object<DataSource>(*this);
    ar & udsf;
  }
};
}  // namespace NES
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(NES::SenseSource)
