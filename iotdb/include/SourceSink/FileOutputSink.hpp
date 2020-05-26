#ifndef FILEOUTPUTSINK_HPP
#define FILEOUTPUTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <SourceSink/DataSink.hpp>

namespace NES {

/**
 * @brief this class implements the File output sink
 * @default default type is Binary and default mode is append
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
    FileOutputSink(SchemaPtr schema, std::string filePath);

    /**
     * @brief constructor that creates an empty file sink using a schema
     * @param schema of the print sink
     * @param filePath location of file on sink server
     * @param type of file to write
     * @param mode to write
     */
    FileOutputSink(SchemaPtr schema, std::string filePath, FileOutputType type, FileOutputMode mode);

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
    bool writeData(TupleBuffer& input_buffer) override;

    /**
     * @brief override the toString method for the file output sink
     * @return returns string describing the file output sink
     */
    const std::string toString() const override;

    /**
     * @brief get file path
     */
    const std::string getFilePath() const;

    /**
     * @brief Get file output type
     */
    FileOutputType getOutputType() const;

    /**
     * @brief Get file output mode
     */
    FileOutputMode getOutputMode() const;

    /**
     * @brief Get sink type
     */
    SinkType getType() const override;

  private:
    std::string filePath;

    friend class boost::serialization::access;

    FileOutputType outputType;
    FileOutputMode outputMode;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& filePath;
        ar& outputType;
        ar& outputMode;
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataSink);
    }
};
typedef std::shared_ptr<FileOutputSink> FileOutputSinkPtr;
}// namespace NES

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

BOOST_CLASS_EXPORT_KEY(NES::FileOutputSink)

#endif// FILEOUTPUTSINK_HPP
