#include "Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp"

namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency)
    : SourceDescriptor(schema), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

SourceDescriptorType DefaultSourceDescriptor::getType() {
    return DefaultSource;
}

const uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const {
    return numbersOfBufferToProduce;
}

const uint32_t DefaultSourceDescriptor::getFrequency() const {
    return frequency;
}

}


