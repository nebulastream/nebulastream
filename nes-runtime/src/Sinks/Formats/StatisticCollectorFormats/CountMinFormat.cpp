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

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollectorFormats/CountMinFormat.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Statistics/CountMin.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

std::vector<StatisticPtr> CountMinFormat::readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer) {
    std::vector<StatisticPtr> statisticCollectors = {};

    for (uint64_t rowIdx = 0; rowIdx < dynBuffer.getNumberOfTuples(); rowIdx++) {
        // read and create statisticCollectorIdentifier
        auto logicalSourceName = Runtime::MemoryLayouts::readVarSizedData(
            dynBuffer.getBuffer(),
            dynBuffer[rowIdx][LOGICAL_SOURCE_NAME].read<Runtime::TupleBuffer::NestedTupleBufferKey>());
        auto physicalSourceName = Runtime::MemoryLayouts::readVarSizedData(
            dynBuffer.getBuffer(),
            dynBuffer[rowIdx][PHYSICAL_SOURCE_NAME].read<Runtime::TupleBuffer::NestedTupleBufferKey>());
        auto workerId = dynBuffer[rowIdx][WORKER_ID].read<uint64_t>();
        auto fieldName = Runtime::MemoryLayouts::readVarSizedData(
            dynBuffer.getBuffer(),
            dynBuffer[rowIdx][FIELD_NAME].read<Runtime::TupleBuffer::NestedTupleBufferKey>());

        auto statisticCollectorIdentifier = std::make_shared<StatisticCollectorIdentifier>(logicalSourceName,
                                                                                           physicalSourceName,
                                                                                           workerId,
                                                                                           fieldName,
                                                                                           StatisticCollectorType::COUNT_MIN);

        // read other general statisticCollector fields
        auto observedTuples = dynBuffer[rowIdx][OBSERVED_TUPLES].read<uint64_t>();
        auto depth = dynBuffer[rowIdx][DEPTH].read<uint64_t>();
        auto startTime = dynBuffer[rowIdx][START_TIME].read<uint64_t>();
        auto endTime = dynBuffer[rowIdx][END_TIME].read<uint64_t>();

        // read sketch data
        auto synopsesText =
            Runtime::MemoryLayouts::readVarSizedData(dynBuffer.getBuffer(),
                                                     dynBuffer[rowIdx][DATA].read<Runtime::TupleBuffer::NestedTupleBufferKey>());

        // read sketch specific fields
        auto width = dynBuffer[rowIdx][WIDTH].read<uint64_t>();

        // convert Text back to array
        std::vector<uint64_t> data(depth * width, 0);
        memcpy(data.data(), synopsesText.data(), synopsesText.size());

        // create sketch and add it to the vector of sketches
        auto countMin =
            std::make_shared<CountMin>(width, data, statisticCollectorIdentifier, observedTuples, depth, startTime, endTime);
        statisticCollectors.push_back(countMin);
    }

    return statisticCollectors;
}
}// namespace NES::Experimental::Statistics
