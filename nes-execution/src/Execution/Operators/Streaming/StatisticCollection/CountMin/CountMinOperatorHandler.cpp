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

#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinOperatorHandler.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <random>

namespace NES::Runtime::Execution::Operators {
CountMinOperatorHandlerPtr CountMinOperatorHandler::create(const uint64_t windowSize,
                                                           const uint64_t windowSlide,
                                                           Statistic::SendingPolicyPtr sendingPolicy,
                                                           const uint64_t width,
                                                           const uint64_t depth,
                                                           Statistic::AbstractStatisticFormatPtr statisticFormat,
                                                           const std::vector<OriginId>& inputOrigins,
                                                           const uint64_t numberOfBitsInKey) {
    return std::make_shared<CountMinOperatorHandler>(CountMinOperatorHandler(windowSize, windowSlide,
                                                                             sendingPolicy,
                                                                             width, depth, statisticFormat,
                                                                             inputOrigins, numberOfBitsInKey));
}

const std::vector<uint64_t>& CountMinOperatorHandler::getH3Seeds() const { return h3Seeds; }

CountMinOperatorHandler::CountMinOperatorHandler(const uint64_t windowSize,
                                                 const uint64_t windowSlide,
                                                 Statistic::SendingPolicyPtr sendingPolicy,
                                                 const uint64_t width,
                                                 const uint64_t depth,
                                                 Statistic::AbstractStatisticFormatPtr statisticFormat,
                                                 const std::vector<OriginId>& inputOrigins,
                                                 const uint64_t numberOfBitsInKey)
    : AbstractSynopsesOperatorHandler(windowSize, windowSlide, sendingPolicy, statisticFormat, inputOrigins),
      width(width), depth(depth), numberOfBitsInKey(numberOfBitsInKey) {

    // Creating here the H3-Seeds with a custom seed, allowing us to not have to send the seed with the statistics
    std::random_device rd;
    std::mt19937 gen(H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;
    for (auto row = 0UL; row < depth; ++row) {
        for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
            h3Seeds.emplace_back(distribution(gen));
        }
    }
}

Statistic::StatisticPtr CountMinOperatorHandler::createInitStatistic(Windowing::TimeMeasure sliceStart,
                                                                     Windowing::TimeMeasure sliceEnd) {
    return Statistic::CountMinStatistic::createInit(sliceStart, sliceEnd, width, depth, numberOfBitsInKey);
}

}// namespace NES::Runtime::Execution::Operators