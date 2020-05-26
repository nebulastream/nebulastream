#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>

namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency)
    : SourceDescriptor(schema), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const {
    return numbersOfBufferToProduce;
}

uint32_t DefaultSourceDescriptor::getFrequency() const {
    return frequency;
}

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema,
                                                    uint64_t numbersOfBufferToProduce,
                                                    uint32_t frequency) {
    return std::make_shared<DefaultSourceDescriptor>(DefaultSourceDescriptor(schema,
                                                                             numbersOfBufferToProduce,
                                                                             frequency));
}

}// namespace NES
