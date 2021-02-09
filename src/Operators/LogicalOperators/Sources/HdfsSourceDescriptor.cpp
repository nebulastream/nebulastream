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
#include "Operators/LogicalOperators/Sources/HdfsSourceDescriptor.hpp"
#include <utility>
namespace NES {

HdfsSourceDescriptor::HdfsSourceDescriptor(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                           std::string filePath, std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                           uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader)
    : SourceDescriptor(std::move(schema)), namenode(std::move(namenode)), port(port), hadoopUser(hadoopUser), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader) {}

HdfsSourceDescriptor::HdfsSourceDescriptor(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                           std::string hadoopUser, std::string filePath, std::string delimiter,
                                           uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                           uint64_t frequency, bool skipHeader)
    : SourceDescriptor(std::move(schema), std::move(streamName)), namenode(std::move(namenode)), port(port),
      hadoopUser(std::move(hadoopUser)), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader) {}

SourceDescriptorPtr HdfsSourceDescriptor::create(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                                 std::string filePath, std::string delimiter,
                                                 uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                 uint64_t frequency, bool skipHeader) {
    return std::make_shared<HdfsSourceDescriptor>(HdfsSourceDescriptor(std::move(schema), std::move(namenode),
                                                                       port, std::move(hadoopUser), std::move(filePath),
                                                                       std::move(delimiter), numberOfTuplesToProducePerBuffer,
                                                                       numBuffersToProcess, frequency, skipHeader));
}

SourceDescriptorPtr HdfsSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                                 std::string hadoopUser, std::string filePath, std::string delimiter,
                                                 uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                 uint64_t frequency, bool skipHeader) {
    return std::make_shared<HdfsSourceDescriptor>(
        HdfsSourceDescriptor(std::move(schema), std::move(streamName), std::move(namenode), port, std::move(hadoopUser),
                             std::move(filePath), std::move(delimiter), numberOfTuplesToProducePerBuffer, numBuffersToProcess,
                             frequency, skipHeader));
}

const std::string& HdfsSourceDescriptor::getNamenode() const { return namenode; }

uint64_t HdfsSourceDescriptor::getPort() const { return port; }

const std::string& HdfsSourceDescriptor::getHadoopUser() const { return hadoopUser; }

const std::string& HdfsSourceDescriptor::getFilePath() const { return filePath; }

const std::string& HdfsSourceDescriptor::getDelimiter() const { return delimiter; }

bool HdfsSourceDescriptor::getSkipHeader() const { return skipHeader; }

uint64_t HdfsSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t HdfsSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint64_t HdfsSourceDescriptor::getFrequency() const { return frequency; }

bool HdfsSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<HdfsSourceDescriptor>())
        return false;
    auto otherSource = other->as<HdfsSourceDescriptor>();
    return filePath == otherSource->getFilePath() && namenode == otherSource->getNamenode() && port == otherSource->getPort() && delimiter == otherSource->getDelimiter()
           && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency()
           && getSchema()->equals(otherSource->getSchema());
}

std::string HdfsSourceDescriptor::toString() {
    return "HdfsSourceDescriptor(" + namenode + "," + std::to_string(port) + "," + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", "
           + std::to_string(frequency) + ")";
}

}// namespace NES
