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

#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogOperatorHandler.hpp>
#include <Statistics/Synopses/HyperLogLogStatistic.hpp>

namespace NES::Runtime::Execution::Operators {

Statistic::StatisticPtr HyperLogLogOperatorHandler::createInitStatistic(Windowing::TimeMeasure sliceStart,
                                                                        Windowing::TimeMeasure sliceEnd) {
    return Statistic::HyperLogLogStatistic::createInit(sliceStart, sliceEnd, width);
}

HyperLogLogOperatorHandlerPtr HyperLogLogOperatorHandler::create(const uint64_t windowSize,
                                                                 const uint64_t windowSlide,
                                                                 Statistic::SendingPolicyPtr sendingPolicy,
                                                                 Statistic::StatisticFormatPtr statisticFormat,
                                                                 const uint64_t width,
                                                                 const std::vector<OriginId>& inputOrigins) {
    return std::make_shared<HyperLogLogOperatorHandler>(HyperLogLogOperatorHandler(windowSize, windowSlide,
                                                                                   sendingPolicy, statisticFormat,
                                                                                   inputOrigins, width));
}

HyperLogLogOperatorHandler::HyperLogLogOperatorHandler(const uint64_t windowSize,
                                                       const uint64_t windowSlide,
                                                       const Statistic::SendingPolicyPtr& sendingPolicy,
                                                       const Statistic::StatisticFormatPtr& statisticFormat,
                                                       const std::vector<OriginId>& inputOrigins,
                                                       const uint64_t width)
    : AbstractSynopsesOperatorHandler(windowSize, windowSlide, sendingPolicy, statisticFormat, inputOrigins), width(width) {}

} // namespace NES::Runtime::Execution::Operators