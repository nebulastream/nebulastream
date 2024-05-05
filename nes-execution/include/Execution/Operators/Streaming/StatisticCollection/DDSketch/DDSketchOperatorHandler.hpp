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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_DDSKETCH_DDSKETCHOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_DDSKETCH_DDSKETCHOPERATORHANDLER_HPP_

#include <Execution/Operators/Streaming/StatisticCollection/AbstractSynopsesOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {

class DDSketchOperatorHandler;
using DDSketchOperatorHandlerPtr = std::shared_ptr<DDSketchOperatorHandler>;

class DDSketchOperatorHandler : public AbstractSynopsesOperatorHandler {
  public:

    static DDSketchOperatorHandlerPtr create(const uint64_t windowSize,
                                             const uint64_t windowSlide,
                                             Statistic::SendingPolicyPtr sendingPolicy,
                                             const uint64_t numberOfBuckets,
                                             const double gamma,
                                             Statistic::StatisticFormatPtr statisticFormat,
                                             const std::vector<OriginId>& inputOrigins);

    Statistic::StatisticPtr createInitStatistic(Windowing::TimeMeasure sliceStart, Windowing::TimeMeasure sliceEnd) override;

  private:
    DDSketchOperatorHandler(const uint64_t windowSize,
                            const uint64_t windowSlide,
                            Statistic::SendingPolicyPtr sendingPolicy,
                            const uint64_t numberOfBuckets,
                            const double gamma,
                            Statistic::StatisticFormatPtr statisticFormat,
                            const std::vector<OriginId>& inputOrigins);
    const uint64_t numberOfBuckets;
    const double gamma;
};
} // namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_DDSKETCH_DDSKETCHOPERATORHANDLER_HPP_
