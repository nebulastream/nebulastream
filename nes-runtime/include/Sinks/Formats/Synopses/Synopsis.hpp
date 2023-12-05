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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSIS_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSIS_HPP_

#include <Identifiers.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <string>
#include <vector>

namespace NES {

namespace Runtime::MemoryLayouts {
class DynamicTupleBuffer;
}

namespace Experimental::Statistics {

class Synopsis {
  public:
    Synopsis(const std::string& logicalSourceName,
             const std::string& physicalSourceName,
             const std::string& fieldName,
             TopologyNodeId nodeId,
             uint64_t statisticCollectorStorageKey,
             uint64_t observedTuples,
             time_t startTime,
             time_t endTime);

    virtual ~Synopsis() = default;

    virtual void writeToBuffer(std::vector<Synopsis>& synopses, Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer) = 0;

    virtual std::vector<Synopsis> readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer) = 0;

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    std::string fieldName;
    TopologyNodeId nodeId;
    uint64_t statisticCollectorStorageKey;
    uint64_t observedTuples;
    time_t startTime;
    time_t endTime;
};
}
}// namespace NES::Experimental::Statistics

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSIS_HPP_
