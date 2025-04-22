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
#include <Util/CheckpointStorageType.hpp>

namespace NES {
SinkDescriptor::SinkDescriptor() : SinkDescriptor(1) {}

SinkDescriptor::SinkDescriptor(uint64_t numberOfOrigins) : SinkDescriptor(numberOfOrigins, false) {}

SinkDescriptor::SinkDescriptor(uint64_t numberOfOrigins, bool addTimestamp)
    : numberOfOrigins(numberOfOrigins), addTimestamp(addTimestamp) {}

FaultToleranceType SinkDescriptor::getFaultToleranceType() const { return faultToleranceType; }

void SinkDescriptor::setFaultToleranceType(FaultToleranceType faultTolerance) { faultToleranceType = faultTolerance; }

CheckpointStorageType SinkDescriptor::getCheckpointStorageType() const { return checkpointStorageType; }

void SinkDescriptor::setCheckpointStorageType(CheckpointStorageType checkpointStorage) { checkpointStorageType = checkpointStorage; }

uint64_t SinkDescriptor::getNumberOfOrigins() const { return numberOfOrigins; }

bool SinkDescriptor::getAddTimestamp() const { return addTimestamp; }
}// namespace NES
