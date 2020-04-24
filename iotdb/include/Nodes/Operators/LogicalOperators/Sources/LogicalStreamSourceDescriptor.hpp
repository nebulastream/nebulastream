#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining referencing a logical source stream.
 */
class LogicalStreamSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(std::string streamName);
    SourceDescriptorType getType() override;
    const std::string& getStreamName() const;

  private:
    LogicalStreamSourceDescriptor(std::string streamName);
    std::string streamName;
};

typedef std::shared_ptr<LogicalStreamSourceDescriptor> LogicalStreamSourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SREAMSOURCEDESCRIPTOR_HPP_
