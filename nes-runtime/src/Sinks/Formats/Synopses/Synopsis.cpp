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

#include <Sinks/Formats/Synopses/Synopsis.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>

namespace NES::Experimental::Statistics {

Synopsis::Synopsis(const std::string& logicalSourceName,
                   const std::string& physicalSourceName,
                   const std::string& fieldName,
                   const NES::TopologyNodeId nodeId,
                   const uint64_t statisticCollectorStorageKey,
                   const uint64_t observedTuples,
                   const time_t startTime,
                   const time_t endTime)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName), fieldName(fieldName), nodeId(nodeId),
      statisticCollectorStorageKey(statisticCollectorStorageKey), observedTuples(observedTuples), startTime(startTime),
      endTime(endTime) {}

}// namespace NES::Experimental::Statistics