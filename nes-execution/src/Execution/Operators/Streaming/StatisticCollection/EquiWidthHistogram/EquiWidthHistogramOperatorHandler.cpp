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

#include <Execution/Operators/Streaming/StatisticCollection/EquiWidthHistogram/EquiWidthHistogramOperatorHandler.hpp>
#include <Statistics/Synopses/EquiWidthHistogramStatistic.hpp>

namespace NES::Runtime::Execution::Operators {

EquiWidthHistogramOperatorHandlerPtr EquiWidthHistogramOperatorHandler::create(const uint64_t windowSize,
                                                                               const uint64_t windowSlide,
                                                                               Statistic::SendingPolicyPtr sendingPolicy,
                                                                               const uint64_t binWidth,
                                                                               Statistic::StatisticFormatPtr statisticFormat,
                                                                               const std::vector<OriginId>& inputOrigins) {
    return std::make_shared<EquiWidthHistogramOperatorHandler>(EquiWidthHistogramOperatorHandler(windowSize, windowSlide, sendingPolicy, binWidth, statisticFormat, inputOrigins));
}

Statistic::StatisticPtr EquiWidthHistogramOperatorHandler::createInitStatistic(Windowing::TimeMeasure sliceStart,
                                                                               Windowing::TimeMeasure sliceEnd) {
    return Statistic::EquiWidthHistogramStatistic::createInit(sliceStart, sliceEnd, binWidth);
}

EquiWidthHistogramOperatorHandler::EquiWidthHistogramOperatorHandler(const uint64_t windowSize,
                                                                     const uint64_t windowSlide,
                                                                     Statistic::SendingPolicyPtr sendingPolicy,
                                                                     const uint64_t binWidth,
                                                                     Statistic::StatisticFormatPtr statisticFormat,
                                                                     const std::vector<OriginId>& inputOrigins)
: AbstractSynopsesOperatorHandler(windowSize, windowSlide, sendingPolicy, statisticFormat, inputOrigins), binWidth(binWidth) {}

} // namespace NES::Runtime::Execution::Operators