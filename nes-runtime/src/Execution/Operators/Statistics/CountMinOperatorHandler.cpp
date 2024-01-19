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

#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>

namespace NES::Experimental::Statistics {

CountMinOperatorHandler::CountMinOperatorHandler(uint64_t windowSize, uint64_t slideFactor, uint64_t metaDataSize)
    : sliceAssigner(windowSize, slideFactor), allCountMin(), allMetaData(), metaDataSize(metaDataSize) {}

void CountMinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {
    NES_DEBUG("start CountMinOperatorHandler");
}

void CountMinOperatorHandler::stop(Runtime::QueryTerminationType terminationType,
                                   Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("shutdown CountMinOperatorHandler: {}", terminationType);
}

bool CountMinOperatorHandler::countMinExists(uint64_t ts) {
    auto startTime = sliceAssigner.getSliceStartTs(ts);
    auto endTime = sliceAssigner.getSliceEndTs(ts);
    auto interval = Interval(startTime, endTime);
    auto countMinPair = allCountMin.find(interval);
    if (countMinPair != allCountMin.end()) {
        return true;
    } else {
        return false;
    }
}

void CountMinOperatorHandler::createAndInsertCountMin(uint64_t depth,
                                                      uint64_t width,
                                                      StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                                                      uint64_t ts) {
    std::vector<std::vector<uint64_t>> data(depth, std::vector<uint64_t>(width, 0));
    auto startTime = sliceAssigner.getSliceStartTs(ts);
    auto endTime = sliceAssigner.getSliceEndTs(ts);
    auto interval = Interval(startTime, endTime);
    auto countMinSketch = std::make_shared<CountMin>(width, data, statisticCollectorIdentifier, 0, depth, startTime, endTime);
    allCountMin[interval] = countMinSketch;
}

void CountMinOperatorHandler::incrementCounter(uint64_t rowId, uint64_t colId, uint64_t ts) {
    auto startTime = sliceAssigner.getSliceStartTs(ts);
    auto endTime = sliceAssigner.getSliceEndTs(ts);
    auto interval = Interval(startTime, endTime);
    auto countMinPair = allCountMin.find(interval);
    auto countMin = countMinPair->second.get();
    countMin->increment(rowId, colId);
}

CountMinPtr CountMinOperatorHandler::getCountMin(Interval& interval) {
    auto countMinPair = allCountMin.find(interval);
    if (countMinPair != allCountMin.end()) {
        return countMinPair->second;
    } else {
        NES_ERROR("No CountMin found for the defined window/interval");
        return nullptr;
    }
}


uint64_t CountMinOperatorHandler::getMetaDataSize() const { return metaDataSize; }

}// namespace NES::Experimental::Statistics