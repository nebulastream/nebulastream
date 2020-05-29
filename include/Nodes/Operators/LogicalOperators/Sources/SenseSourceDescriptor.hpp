#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical sense source
 */
class SenseSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string udfs);

    /**
     * @brief Get the udf for the sense node
     */
    const std::string& getUdfs() const;
    bool equal(SourceDescriptorPtr other) override;
    const std::string& toString() override;

  private:
    explicit SenseSourceDescriptor(SchemaPtr schema, std::string udfs);

    std::string udfs;
};

typedef std::shared_ptr<SenseSourceDescriptor> SenseSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
