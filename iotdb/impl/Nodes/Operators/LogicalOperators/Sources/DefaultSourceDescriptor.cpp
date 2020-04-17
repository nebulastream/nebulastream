#include "Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp"

namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(uint64_t numbersOfBufferToProduce, uint32_t frequency)
    : numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

SourceDescriptorType DefaultSourceDescriptor::getType() {
    return DefaultDescriptor;
}

const uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const {
    return numbersOfBufferToProduce;
}

const uint32_t DefaultSourceDescriptor::getFrequency() const {
    return frequency;
}

}


