#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical default source
 */
class DefaultSourceDescriptor : public SourceDescriptor {

  public:

    DefaultSourceDescriptor(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint32_t frequency);

    SourceDescriptorType getType() override;

    /**
     * @brief Get number of buffers to be produced
     */
    const uint64_t getNumbersOfBufferToProduce() const;

    /**
     * @brief Get the frequency to produce the buffers
     */
    const uint32_t getFrequency() const;

  private:

    const uint64_t numbersOfBufferToProduce;
    const uint32_t frequency;
};

typedef std::shared_ptr<DefaultSourceDescriptor> DefaultSourceDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
