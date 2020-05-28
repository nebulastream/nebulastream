#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical file sink
 */
class FileSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief Factory method to create a new file sink descriptor
     * @param schema output schema of this sink descriptor
     * @param filePath the path to the output file
     * @return descriptor for file sink
     */
    static SinkDescriptorPtr create(std::string filePath);

    /**
     * @brief Get the file name where the data is to be written
     */
    const std::string& getFileName() const;

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    explicit FileSinkDescriptor(std::string fileName);
    std::string fileName;
};

typedef std::shared_ptr<FileSinkDescriptor> FileSinkDescriptorPtr;
}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_FILESINKDESCRIPTOR_HPP_
