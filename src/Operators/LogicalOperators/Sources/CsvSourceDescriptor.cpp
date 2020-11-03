#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <utility>
namespace NES {

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema,
                                         std::string filePath,
                                         std::string delimiter,
                                         size_t numberOfTuplesToProducePerBuffer,
                                         size_t numBuffersToProcess,
                                         size_t frequency,
                                         bool endlessRepeat)
    : SourceDescriptor(std::move(schema)),
      filePath(std::move(filePath)),
      delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      numBuffersToProcess(numBuffersToProcess),
      frequency(frequency),
      endlessRepeat(endlessRepeat) {}

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, std::string delimiter,
                                         size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess, size_t frequency, bool endlessRepeat)
    : SourceDescriptor(std::move(schema), std::move(streamName)),
      filePath(std::move(filePath)),
      delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      numBuffersToProcess(numBuffersToProcess),
      frequency(frequency),
      endlessRepeat(endlessRepeat) {}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema,
                                                std::string filePath,
                                                std::string delimiter,
                                                size_t numberOfTuplesToProducePerBuffer,
                                                size_t numBuffersToProcess,
                                                size_t frequency,
                                                bool endlessRepeat) {
    return std::make_shared<CsvSourceDescriptor>(CsvSourceDescriptor(std::move(schema), std::move(filePath), std::move(delimiter),
                                                                     numberOfTuplesToProducePerBuffer, numBuffersToProcess, frequency, endlessRepeat));
}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema,
                                                std::string streamName,
                                                std::string filePath,
                                                std::string delimiter,
                                                size_t numberOfTuplesToProducePerBuffer,
                                                size_t numBuffersToProcess,
                                                size_t frequency,
                                                bool endlessRepeat) {
    return std::make_shared<CsvSourceDescriptor>(CsvSourceDescriptor(std::move(schema), std::move(streamName), std::move(filePath), std::move(delimiter), numberOfTuplesToProducePerBuffer, numBuffersToProcess, frequency, endlessRepeat));
}

const std::string& CsvSourceDescriptor::getFilePath() const {
    return filePath;
}

const std::string& CsvSourceDescriptor::getDelimiter() const {
    return delimiter;
}

size_t CsvSourceDescriptor::getNumBuffersToProcess() const {
    return numBuffersToProcess;
}

size_t CsvSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}

size_t CsvSourceDescriptor::getFrequency() const {
    return frequency;
}

bool CsvSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<CsvSourceDescriptor>())
        return false;
    auto otherSource = other->as<CsvSourceDescriptor>();
    return filePath == otherSource->getFilePath() && delimiter == otherSource->getDelimiter() && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency() && getSchema()->equals(otherSource->getSchema());
}

std::string CsvSourceDescriptor::toString() {
    return "CsvSourceDescriptor(" + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", " + std::to_string(frequency) + ")";
}
bool CsvSourceDescriptor::isEndlessRepeat() const {
    return endlessRepeat;
}
void CsvSourceDescriptor::setEndlessRepeat(bool endlessRepeat) {
    this->endlessRepeat = endlessRepeat;
}

}// namespace NES
