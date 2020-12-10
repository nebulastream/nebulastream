/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <utility>
namespace NES {

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema, std::string filePath, std::string delimiter,
                                         uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                         uint64_t frequency, bool skipHeader, OperatorId operatorId)
    : SourceDescriptor(std::move(schema), operatorId), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader) {}

CsvSourceDescriptor::CsvSourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, std::string delimiter,
                                         uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                         uint64_t frequency, bool skipHeader, OperatorId operatorId)
    : SourceDescriptor(std::move(schema), std::move(streamName), operatorId), filePath(std::move(filePath)),
      delimiter(std::move(delimiter)), numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer),
      numBuffersToProcess(numBuffersToProcess), frequency(frequency), skipHeader(skipHeader) {}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema, std::string filePath, std::string delimiter,
                                                uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                uint64_t frequency, bool skipHeader, OperatorId operatorId) {
    return std::make_shared<CsvSourceDescriptor>(CsvSourceDescriptor(std::move(schema), std::move(filePath), std::move(delimiter),
                                                                     numberOfTuplesToProducePerBuffer, numBuffersToProcess,
                                                                     frequency, skipHeader, operatorId));
}

SourceDescriptorPtr CsvSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string filePath,
                                                std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                                uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader,
                                                OperatorId operatorId) {
    return std::make_shared<CsvSourceDescriptor>(
        CsvSourceDescriptor(std::move(schema), std::move(streamName), std::move(filePath), std::move(delimiter),
                            numberOfTuplesToProducePerBuffer, numBuffersToProcess, frequency, skipHeader, operatorId));
}

const std::string& CsvSourceDescriptor::getFilePath() const { return filePath; }

const std::string& CsvSourceDescriptor::getDelimiter() const { return delimiter; }

bool CsvSourceDescriptor::getSkipHeader() const { return skipHeader; }

uint64_t CsvSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t CsvSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint64_t CsvSourceDescriptor::getFrequency() const { return frequency; }

bool CsvSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<CsvSourceDescriptor>())
        return false;
    auto otherSource = other->as<CsvSourceDescriptor>();
    return filePath == otherSource->getFilePath() && delimiter == otherSource->getDelimiter()
        && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency()
        && getSchema()->equals(otherSource->getSchema());
}

std::string CsvSourceDescriptor::toString() {
    return "CsvSourceDescriptor(" + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", "
        + std::to_string(frequency) + ")";
}

}// namespace NES
