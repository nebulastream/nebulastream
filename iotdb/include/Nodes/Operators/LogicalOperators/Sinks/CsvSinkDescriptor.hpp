#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_CSVSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_CSVSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating csv sink.
 */
class CsvSinkDescriptor : public SinkDescriptor {

  public:
    enum FileOutputMode {
        OVERWRITE,
        APPEND
    };
    /**
     * @brief Factory method to create a new csv sink descriptor
     * @param schema output schema of this sink descriptor
     * @param filePath the path to the output file
     * @param fileOutputMode the file output mode (OVERWRITE, APPEND)
     * @return descriptor for file sink
     */
    static SinkDescriptorPtr create(std::string filePath, FileOutputMode fileOutputMode);

    /**
     * @brief Get the file name where the data is to be written
     */
    const std::string& getFileName() const;

    /**
     * @brief get the file output mode (Overwritten or Append)
     */
    CsvSinkDescriptor::FileOutputMode getFileOutputMode() const;

  private:
    explicit CsvSinkDescriptor(std::string fileName, FileOutputMode fileOutputMode);

    std::string fileName;
    FileOutputMode fileOutputMode;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_CSVSINKDESCRIPTOR_HPP_
