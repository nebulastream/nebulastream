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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSES_COUNTMIN_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSES_COUNTMIN_HPP_

#include <Sinks/Formats/Synopses/Synopsis.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <vector>

namespace NES::Experimental::Statistics {

class CountMin : public Synopsis {
  public:
    CountMin(uint64_t** data,
             double error,
             double probability,
             const std::string& logicalSourceName,
             const std::string& physicalSourceName,
             const std::string& fieldName,
             const TopologyNodeId nodeId,
             const uint64_t statisticCollectorStorageKey,
             const uint64_t observedTuples,
             const time_t startTime,
             const time_t endTime);

    virtual ~CountMin() = default;

    static uint64_t calcDepth(double probability);

    static uint64_t calcWidth(double error);

//    CountMin& createCountMinFromTuple(uint64_t depth, uint64_t width, uint8_t* startAddress, uint64_t sizeOfTextField);

    void writeToBuffer(std::vector<Synopsis>& synopses, Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) override;

    std::vector<Synopsis> readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) override;

    uint64_t getDepth() const;

    uint64_t getWidth() const;

    double getError() const;

    double getProbability() const;

  private:
    uint64_t** data;
    uint64_t depth;
    uint64_t width;
    double error;
    double probability;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_SYNOPSES_COUNTMIN_HPP_
