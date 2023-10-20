/*
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

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {
SinkDescriptor::SinkDescriptor() : SinkDescriptor(FaultToleranceType::NONE, 1) {}

SinkDescriptor::SinkDescriptor(FaultToleranceType faultToleranceType, uint64_t numberOfOrigins)
    : SinkDescriptor(faultToleranceType, numberOfOrigins, false) {}

SinkDescriptor::SinkDescriptor(FaultToleranceType faultToleranceType, uint64_t numberOfOrigins, bool addTimestamp)
    : faultToleranceType(faultToleranceType), numberOfOrigins(numberOfOrigins), addTimestamp(addTimestamp), numberOfInputSources(0) {}

FaultToleranceType SinkDescriptor::getFaultToleranceType() const { return faultToleranceType; }

void SinkDescriptor::setFaultToleranceType(FaultToleranceType faultTolerance) { faultToleranceType = faultTolerance; }

uint64_t SinkDescriptor::getNumberOfOrigins() const { return numberOfOrigins; }

bool SinkDescriptor::getAddTimestamp() const { return addTimestamp; }

uint16_t SinkDescriptor::getNumberOfInputSources() const { return numberOfInputSources; }

void SinkDescriptor::setNumberOfInputSources(uint16_t numberOfSources) {
    this->numberOfInputSources = numberOfSources;
}

}// namespace NES
