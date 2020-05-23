#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining referencing a logical source stream.
 */
class LogicalStreamSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief Factory method to create a new logical source stream descriptor.
     * @param streamName Name of this stream
     * @return SourceDescriptorPtr
     */
    static SourceDescriptorPtr create(std::string streamName);

    /**
     * @brief Name of the logical stream
     * @return name
     */
    const std::string& getStreamName() const;
    bool equal(SourceDescriptorPtr other) override;
    const std::string& toString() override;

  private:
    explicit LogicalStreamSourceDescriptor(std::string streamName);
    std::string streamName;
};

typedef std::shared_ptr<LogicalStreamSourceDescriptor> LogicalStreamSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_
