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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINOPERATORHANDLER_HPP_
#include <Execution/Operators/Streaming/StatisticCollection/AbstractSynopsesOperatorHandler.hpp>
#include <Identifiers.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators {

class CountMinOperatorHandler;
using CountMinOperatorHandlerPtr = std::shared_ptr<CountMinOperatorHandler>;

/**
 * @brief Operatorhandler for a CountMinBuild operator. It stores all count min sketches in a StatisticStore.
 */
class CountMinOperatorHandler : public AbstractSynopsesOperatorHandler {
  public:
    static CountMinOperatorHandlerPtr create(const uint64_t windowSize,
                                             const uint64_t windowSlide,
                                             Statistic::SendingPolicyPtr sendingPolicy,
                                             const uint64_t width,
                                             const uint64_t depth,
                                             Statistic::AbstractStatisticFormatPtr statisticFormat,
                                             const std::vector<OriginId>& inputOrigins,
                                             const uint64_t numberOfBitsInKey);

    Statistic::StatisticPtr createInitStatistic(Windowing::TimeMeasure sliceStart, Windowing::TimeMeasure sliceEnd) override;
    const std::vector<uint64_t>& getH3Seeds() const;

  private:
    CountMinOperatorHandler(const uint64_t windowSize,
                            const uint64_t windowSlide,
                            Statistic::SendingPolicyPtr sendingPolicy,
                            const uint64_t width,
                            const uint64_t depth,
                            Statistic::AbstractStatisticFormatPtr statisticFormat,
                            const std::vector<OriginId>& inputOrigins,
                            const uint64_t numberOfBitsInKey);

    std::vector<uint64_t> h3Seeds;
    const uint64_t width;
    const uint64_t depth;
    const uint64_t numberOfBitsInKey;
};

} // namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_COUNTMIN_COUNTMINOPERATORHANDLER_HPP_
