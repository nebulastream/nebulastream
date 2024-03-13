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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICSINKFORMAT_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICSINKFORMAT_HPP_
#include <Runtime/RuntimeForwardRefs.hpp>
#include <StatisticCollection/StatisticKey.hpp>
#include <memory>
#include <vector>

namespace NES::Statistic {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;
using HashStatisticPair = std::pair<StatisticHash, StatisticPtr>;

class AbstractStatisticSinkFormat;
using AbstractStatisticSinkFormatPtr = std::shared_ptr<AbstractStatisticSinkFormat>;

/**
 * @brief An interface for parsing (reading and creating) statistics from a TupleBuffer. The idea is that this format
 * is called in the StatisticSink and returns multiple statistics that are then inserted into a StatisticStorage
 */
class AbstractStatisticSinkFormat {
  public:
    explicit AbstractStatisticSinkFormat(const Schema& schema, const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout);
    explicit AbstractStatisticSinkFormat(const std::string& qualifierNameWithSeparator,
                                         Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout);

    /**
     * @brief Reads the statistics from the buffer
     * @param buffer: Buffer containing the
     * @return Pairs of <StatisticHash, Statistic>
     */
    virtual std::vector<HashStatisticPair> readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) = 0;

    virtual std::string toString() const = 0;

    virtual ~AbstractStatisticSinkFormat();

  protected:
    const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    const std::string startTsFieldName;
    const std::string endTsFieldName;
    const std::string statisticHashFieldName;
    const std::string statisticTypeFieldName;
    const std::string observedTuplesFieldName;
};
}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICSINKFORMAT_HPP_
