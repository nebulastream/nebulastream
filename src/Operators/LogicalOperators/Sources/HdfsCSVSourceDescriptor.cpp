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

#include "Operators/LogicalOperators/Sources/HdfsCSVSourceDescriptor.hpp"
#include <API/Schema.hpp>
#include <utility>
namespace NES {

HdfsCSVSourceDescriptor::HdfsCSVSourceDescriptor(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                           std::string filePath, std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                                           uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader)
    : SourceDescriptor(std::move(schema)), namenode(std::move(namenode)), port(port), hadoopUser(hadoopUser), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader) {}

HdfsCSVSourceDescriptor::HdfsCSVSourceDescriptor(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                           std::string hadoopUser, std::string filePath, std::string delimiter,
                                           uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                           uint64_t frequency, bool skipHeader)
    : SourceDescriptor(std::move(schema), std::move(streamName)), namenode(std::move(namenode)), port(port),
      hadoopUser(std::move(hadoopUser)), filePath(std::move(filePath)), delimiter(std::move(delimiter)),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numBuffersToProcess(numBuffersToProcess),
      frequency(frequency), skipHeader(skipHeader) {}

SourceDescriptorPtr HdfsCSVSourceDescriptor::create(SchemaPtr schema, std::string namenode, uint64_t port, std::string hadoopUser,
                                                 std::string filePath, std::string delimiter,
                                                 uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                 uint64_t frequency, bool skipHeader) {
    return std::make_shared<HdfsCSVSourceDescriptor>(HdfsCSVSourceDescriptor(std::move(schema), std::move(namenode),
                                                                       port, std::move(hadoopUser), std::move(filePath),
                                                                       std::move(delimiter), numberOfTuplesToProducePerBuffer,
                                                                       numBuffersToProcess, frequency, skipHeader));
}

SourceDescriptorPtr HdfsCSVSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string namenode, uint64_t port,
                                                 std::string hadoopUser, std::string filePath, std::string delimiter,
                                                 uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                                 uint64_t frequency, bool skipHeader) {
    return std::make_shared<HdfsCSVSourceDescriptor>(
        HdfsCSVSourceDescriptor(std::move(schema), std::move(streamName), std::move(namenode), port, std::move(hadoopUser),
                             std::move(filePath), std::move(delimiter), numberOfTuplesToProducePerBuffer, numBuffersToProcess,
                             frequency, skipHeader));
}

const std::string& HdfsCSVSourceDescriptor::getNamenode() const { return namenode; }

uint64_t HdfsCSVSourceDescriptor::getPort() const { return port; }

const std::string& HdfsCSVSourceDescriptor::getHadoopUser() const { return hadoopUser; }

const std::string& HdfsCSVSourceDescriptor::getFilePath() const { return filePath; }

const std::string& HdfsCSVSourceDescriptor::getDelimiter() const { return delimiter; }

bool HdfsCSVSourceDescriptor::getSkipHeader() const { return skipHeader; }

uint64_t HdfsCSVSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

uint64_t HdfsCSVSourceDescriptor::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint64_t HdfsCSVSourceDescriptor::getFrequency() const { return frequency; }

bool HdfsCSVSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<HdfsCSVSourceDescriptor>())
        return false;
    auto otherSource = other->as<HdfsCSVSourceDescriptor>();
    return filePath == otherSource->getFilePath() && namenode == otherSource->getNamenode() && port == otherSource->getPort() && delimiter == otherSource->getDelimiter()
           && numBuffersToProcess == otherSource->getNumBuffersToProcess() && frequency == otherSource->getFrequency()
           && getSchema()->equals(otherSource->getSchema());
}

std::string HdfsCSVSourceDescriptor::toString() {
    return "HdfsCSVSourceDescriptor(" + namenode + "," + std::to_string(port) + "," + filePath + "," + delimiter + ", " + std::to_string(numBuffersToProcess) + ", "
           + std::to_string(frequency) + ")";
}

}// namespace NES
