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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Util/Logger.hpp>
#include <sstream>
namespace NES {

PhysicalStreamConfigPtr PhysicalStreamConfig::create(std::string sourceType, std::string sourceConfig, uint32_t sourceFrequency,
                                                     uint32_t numberOfTuplesToProducePerBuffer, uint32_t numberOfBuffersToProduce,
                                                     std::string physicalStreamName, std::string logicalStreamName,
                                                     bool endlessRepeat, bool skipHeader) {
    return std::make_shared<PhysicalStreamConfig>(
        PhysicalStreamConfig(sourceType, sourceConfig, sourceFrequency, numberOfTuplesToProducePerBuffer,
                             numberOfBuffersToProduce, physicalStreamName, logicalStreamName, endlessRepeat, skipHeader));
}

PhysicalStreamConfig::PhysicalStreamConfig(std::string sourceType, std::string sourceConfig, uint64_t sourceFrequency,
                                           uint64_t numberOfTuplesToProducePerBuffer, uint64_t numberOfBuffersToProduce,
                                           std::string physicalStreamName, std::string logicalStreamName, bool endlessRepeat,
                                           bool skipHeader)
    : sourceType(sourceType), sourceConfig(sourceConfig), sourceFrequency(sourceFrequency),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), numberOfBuffersToProduce(numberOfBuffersToProduce),
      physicalStreamName(physicalStreamName), logicalStreamName(logicalStreamName), endlessRepeat(endlessRepeat),
      skipHeader(skipHeader){};

std::string PhysicalStreamConfig::toString() {
    std::stringstream ss;
    ss << "sourceType=" << sourceType << " sourceConfig=" << sourceConfig << " sourceFrequency=" << sourceFrequency
       << " numberOfTuplesToProducePerBuffer=" << numberOfTuplesToProducePerBuffer
       << " numberOfBuffersToProduce=" << numberOfBuffersToProduce << " physicalStreamName=" << physicalStreamName
       << " logicalStreamName=" << logicalStreamName << " endlessRepeat=" << endlessRepeat;
    return ss.str();
}

const std::string& PhysicalStreamConfig::getSourceType() const { return sourceType; }

const std::string& PhysicalStreamConfig::getSourceConfig() const { return sourceConfig; }

uint32_t PhysicalStreamConfig::getSourceFrequency() const { return sourceFrequency; }

uint32_t PhysicalStreamConfig::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

uint32_t PhysicalStreamConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const std::string PhysicalStreamConfig::getPhysicalStreamName() const { return physicalStreamName; }

const std::string PhysicalStreamConfig::getLogicalStreamName() const { return logicalStreamName; }
bool PhysicalStreamConfig::isEndlessRepeat() const { return endlessRepeat; }
void PhysicalStreamConfig::setEndlessRepeat(bool endlessRepeat) { this->endlessRepeat = endlessRepeat; }

bool PhysicalStreamConfig::getSkipHeader() const { return skipHeader; }
}// namespace NES
