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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICFORMAT_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICFORMAT_HPP_
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <functional>
#include <memory>
#include <vector>

namespace NES::Statistic {

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;
using HashStatisticPair = std::pair<StatisticHash, StatisticPtr>;

class AbstractStatisticFormat;
using StatisticFormatPtr = std::shared_ptr<AbstractStatisticFormat>;

/**
 * @brief An interface for parsing (reading and creating) statistics from/to a TupleBuffer. The idea is that this format
 * is called in the StatisticSink as well as the operator handler and returns multiple statistics that are then
 * inserted into a StatisticStorage
 */
class AbstractStatisticFormat {
  public:
    explicit AbstractStatisticFormat(const Schema& schema,
                                     Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                     std::function<std::string (const std::string&)> postProcessingData,
                                     std::function<std::string (const std::string&)> preProcessingData);

    explicit AbstractStatisticFormat(const std::string& qualifierNameWithSeparator,
                                     Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                     std::function<std::string (const std::string&)> postProcessingData,
                                     std::function<std::string (const std::string&)> preProcessingData);

    /**
     * @brief Reads the statistics from the buffer
     * @param buffer: Buffer containing the
     * @return Pairs of <StatisticHash, Statistic>
     */
    virtual std::vector<HashStatisticPair> readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) = 0;

    /**
     * @brief Writes the statistics to the buffer
     * @param statisticsPlusHashes
     * @param bufferManager
     * @return Vector of tuple buffers containing the sketches
     */
    virtual std::vector<Runtime::TupleBuffer>
    writeStatisticsIntoBuffers(const std::vector<HashStatisticPair>& statisticsPlusHashes,
                               Runtime::BufferManager& bufferManager) = 0;

    [[nodiscard]] virtual std::string toString() const = 0;

    virtual ~AbstractStatisticFormat();

  protected:
    const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    const std::string startTsFieldName;
    const std::string endTsFieldName;
    const std::string statisticHashFieldName;
    const std::string statisticTypeFieldName;
    const std::string observedTuplesFieldName;
    const std::function<std::string (const std::string&)> postProcessingData;
    const std::function<std::string (const std::string&)> preProcessingData;
};
}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_ABSTRACTSTATISTICFORMAT_HPP_
