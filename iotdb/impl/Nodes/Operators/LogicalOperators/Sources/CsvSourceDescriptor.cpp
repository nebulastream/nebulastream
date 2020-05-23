#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
namespace NES {

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema,
                                         std::string filePath,
                                         std::string delimiter,
                                         size_t numBuffersToProcess,
                                         size_t frequency)
    : SourceDescriptor(schema),
      filePath(filePath),
      delimiter(delimiter),
      numBuffersToProcess(numBuffersToProcess),
      frequency(frequency) {}

const std::string& CsvSourceDescriptor::getFilePath() const {
    return filePath;
}

const std::string& CsvSourceDescriptor::getDelimiter() const {
    return delimiter;
}

size_t CsvSourceDescriptor::getNumBuffersToProcess() const {
    return numBuffersToProcess;
}

size_t CsvSourceDescriptor::getFrequency() const {
    return frequency;
}
SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema,
                                                std::string filePath,
                                                std::string delimiter,
                                                size_t numBuffersToProcess,
                                                size_t frequency) {
    return std::make_shared<CsvSourceDescriptor>(CsvSourceDescriptor(schema, filePath, delimiter, numBuffersToProcess, frequency));
}

bool CsvSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<CsvSourceDescriptor>())
        return false;
    auto otherSource = other->as<CsvSourceDescriptor>();
    return filePath == otherSource->getFilePath() && delimiter == otherSource->getDelimiter() && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency() && getSchema()->equals(otherSource->getSchema());
}

const std::string& CsvSourceDescriptor::toString() {
    return "CsvSourceDescriptor(" + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", " + std::to_string(frequency) + ")";
}

}// namespace NES
