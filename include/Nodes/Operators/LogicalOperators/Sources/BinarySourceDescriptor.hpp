#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical binary source
 */
class BinarySourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string filePath);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string filePath);

    /**
     * @brief Get the path of binary file
     * @return
     */
    const std::string& getFilePath() const;

    bool equal(SourceDescriptorPtr other) override;

    std::string toString() override;

  private:
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string filePath);
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath);

    std::string filePath;
};

typedef std::shared_ptr<BinarySourceDescriptor> BinarySourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
