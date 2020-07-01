#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <utility>
namespace NES {

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency)
    : SourceDescriptor(std::move(schema)), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

DefaultSourceDescriptor::DefaultSourceDescriptor(SchemaPtr schema,
                                                 std::string streamName,
                                                 uint64_t numbersOfBufferToProduce,
                                                 uint32_t frequency)
    : SourceDescriptor(std::move(schema), std::move(streamName)), numbersOfBufferToProduce(numbersOfBufferToProduce), frequency(frequency) {}

uint64_t DefaultSourceDescriptor::getNumbersOfBufferToProduce() const {
    return numbersOfBufferToProduce;
}

uint32_t DefaultSourceDescriptor::getFrequency() const {
    return frequency;
}

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema,
                                                    uint64_t numbersOfBufferToProduce,
                                                    uint32_t frequency) {
    return std::make_shared<DefaultSourceDescriptor>(DefaultSourceDescriptor(std::move(schema),
                                                                             numbersOfBufferToProduce,
                                                                             frequency));
}

SourceDescriptorPtr DefaultSourceDescriptor::create(SchemaPtr schema,
                                                    std::string streamName,
                                                    uint64_t numbersOfBufferToProduce,
                                                    uint32_t frequency) {
    return std::make_shared<DefaultSourceDescriptor>(DefaultSourceDescriptor(std::move(schema),
                                                                             std::move(streamName),
                                                                             numbersOfBufferToProduce,
                                                                             frequency));
}
bool DefaultSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<DefaultSourceDescriptor>())
        return false;
    auto otherSource = other->as<DefaultSourceDescriptor>();
    return numbersOfBufferToProduce == otherSource->getNumbersOfBufferToProduce() && frequency == otherSource->getFrequency() && getSchema()->equals(otherSource->getSchema());
}

std::string DefaultSourceDescriptor::toString() {
    return "DefaultSourceDescriptor(" + std::to_string(numbersOfBufferToProduce) + ", " + std::to_string(frequency) + ")";
}
}// namespace NES
