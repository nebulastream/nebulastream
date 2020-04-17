#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class DefaultSourceDescriptor : public SourceDescriptor {

  public:

    DefaultSourceDescriptor(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint32_t frequency);

    SourceDescriptorType getType() override;

    const uint64_t getNumbersOfBufferToProduce() const;
    const uint32_t getFrequency() const;

  private:

    const uint64_t numbersOfBufferToProduce;
    const uint32_t frequency;
};

typedef std::shared_ptr<DefaultSourceDescriptor> DefaultSourceDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
