#ifndef FILEOUTPUTSINK_HPP
#define FILEOUTPUTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

#include <SourceSink/DataSink.hpp>

namespace NES {

enum FileOutPutType { BINARY_TYPE, CSV_TYPE };

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
    FileOutputSink(SchemaPtr schema);

    /**
    * @brief constructor that creates an empty file sink using a schema
    * @param filePath location of file on sink server
    */
    FileOutputSink(std::string filePath);

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param filePath location of file on sink server
     */
    FileOutputSink(SchemaPtr schema, std::string filePath, FileOutPutType type);

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

    SinkType getType() const override;
  private:
    std::string filePath;
    friend class boost::serialization::access;
    FileOutPutType outputType;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar & filePath;
        ar & outputType;
        ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataSink);
    }
};
}  // namespace NES

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(NES::FileOutputSink)

#endif // FILEOUTPUTSINK_HPP
