#pragma once

#include <SourceSink/DataSource.hpp>
#include <fstream>
#include <string>

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
    SenseSource(SchemaPtr schema,
                BufferManagerPtr bufferManager,
                QueryManagerPtr queryManager,
                const std::string& udfs);

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
    const std::string toString() const override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief Get UDFs for sense
     */
    const std::string& getUdsf() const;

  private:
    SenseSource();

    std::string udsf;
    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int version) {
        ar& boost::serialization::base_object<DataSource>(*this);
        ar& udsf;
    }
};

typedef std::shared_ptr<SenseSource> SenseSourcePtr;

}// namespace NES
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(NES::SenseSource)
