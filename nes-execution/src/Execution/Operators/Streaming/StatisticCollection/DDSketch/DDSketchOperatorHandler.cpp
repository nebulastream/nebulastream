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

#include <Execution/Operators/Streaming/StatisticCollection/DDSketch/DDSketchOperatorHandler.hpp>
#include <Statistics/Synopses/DDSketchStatistic.hpp>

namespace NES::Runtime::Execution::Operators {

DDSketchOperatorHandlerPtr DDSketchOperatorHandler::create(const uint64_t windowSize,
                                                           const uint64_t windowSlide,
                                                           Statistic::SendingPolicyPtr sendingPolicy,
                                                           const uint64_t numberOfBuckets,
                                                           const double gamma,
                                                           Statistic::StatisticFormatPtr statisticFormat,
                                                           const std::vector<OriginId>& inputOrigins) {
    return std::make_shared<DDSketchOperatorHandler>(DDSketchOperatorHandler(windowSize, windowSlide, sendingPolicy, numberOfBuckets, gamma,
                                                                            statisticFormat, inputOrigins));
}

Statistic::StatisticPtr DDSketchOperatorHandler::createInitStatistic(Windowing::TimeMeasure sliceStart,
                                                                     Windowing::TimeMeasure sliceEnd) {
    return Statistic::DDSketchStatistic::createInit(sliceStart, sliceEnd, numberOfBuckets, gamma);
}

DDSketchOperatorHandler::DDSketchOperatorHandler(const uint64_t windowSize,
                                                 const uint64_t windowSlide,
                                                 Statistic::SendingPolicyPtr sendingPolicy,
                                                 const uint64_t numberOfBuckets,
                                                 const double gamma,
                                                 Statistic::StatisticFormatPtr statisticFormat,
                                                 const std::vector<OriginId>& inputOrigins)
    : AbstractSynopsesOperatorHandler(windowSize, windowSlide, sendingPolicy, statisticFormat, inputOrigins),
      numberOfBuckets(numberOfBuckets),
      gamma(gamma) {}

} // namespace NES::Runtime::Execution::Operators