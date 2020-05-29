#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include <SourceSink/DataSource.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <fstream>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

namespace NES {

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
    BinarySource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& file_path);

    /**
     * @brief override the receiveData method for the binary source
     * @return returns a buffer if available
     */
    std::optional<TupleBuffer> receiveData() override;

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

    SourceType getType() const override;

    const std::string& getFilePath() const;

  private:
    //this one only required for serialization
    BinarySource();
    std::ifstream input;
    std::string file_path;

    int file_size;
    size_t tuple_size;

    /**
       * @brief method for serialization, all listed variable below are added to the
       * serialization/deserialization process
       */
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar,
                   const unsigned int version) {
        ar& boost::serialization::base_object<DataSource>(*this);
        ar& file_path;
        ar& file_size;
        ar& tuple_size;
        ar& generatedTuples;
        ar& generatedBuffers;
    }
};

typedef std::shared_ptr<BinarySource> BinarySourcePtr;

}// namespace NES
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(NES::BinarySource)
#endif /* INCLUDE_BINARYSOURCE_H_ */
