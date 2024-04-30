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

#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleOperatorHandler.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>

namespace NES::Runtime::Execution::Operators {

ReservoirSampleOperatorHandlerPtr ReservoirSampleOperatorHandler::create(const uint64_t windowSize,
                                                                         const uint64_t windowSlide,
                                                                         Statistic::SendingPolicyPtr sendingPolicy,
                                                                         const uint64_t sampleSize,
                                                                         const MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                                         Statistic::StatisticFormatPtr statisticFormat,
                                                                         const std::vector<OriginId>& inputOrigins) {
    return std::make_shared<ReservoirSampleOperatorHandler>(ReservoirSampleOperatorHandler(windowSize, windowSlide,
                                                                                           sendingPolicy, sampleSize,
                                                                                           memoryLayout, statisticFormat,
                                                                                           inputOrigins));
}

Statistic::StatisticPtr ReservoirSampleOperatorHandler::createInitStatistic(Windowing::TimeMeasure sliceStart,
                                                                            Windowing::TimeMeasure sliceEnd) {
    return Statistic::ReservoirSampleStatistic::createInit(sliceStart, sliceEnd, sampleSize, memoryLayout->getSchema());
}

ReservoirSampleOperatorHandler::ReservoirSampleOperatorHandler(const uint64_t windowSize,
                                                               const uint64_t windowSlide,
                                                               Statistic::SendingPolicyPtr sendingPolicy,
                                                               const uint64_t sampleSize,
                                                               const MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                                               Statistic::StatisticFormatPtr statisticFormat,
                                                               const std::vector<OriginId>& inputOrigins)
    : AbstractSynopsesOperatorHandler(windowSize, windowSlide, sendingPolicy, statisticFormat, inputOrigins),
      sampleSize(sampleSize), memoryLayout(std::move(memoryLayout)) {}

}// namespace NES::Runtime::Execution::Operators