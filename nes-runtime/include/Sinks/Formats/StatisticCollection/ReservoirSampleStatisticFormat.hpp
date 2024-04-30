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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_RESERVOIRSAMPLESTATISTICFORMAT_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_RESERVOIRSAMPLESTATISTICFORMAT_HPP_

#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>


namespace NES::Statistic {
/**
 * @brief StatisticSinkFormat that creates/builds Reservoir Samples from a tuple buffer
 */
class ReservoirSampleStatisticFormat : public AbstractStatisticFormat {
  public:
    static StatisticFormatPtr create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                             std::function<std::string (const std::string&)> postProcessingData,
                                             std::function<std::string (const std::string&)> preProcessingData);

    std::vector<std::pair<StatisticHash, StatisticPtr>> readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) override;
    std::vector<Runtime::TupleBuffer> writeStatisticsIntoBuffers(const std::vector<HashStatisticPair>& statisticsPlusHashes,
                                                                 Runtime::BufferManager& bufferManager) override;
    [[nodiscard]] std::string toString() const override;
    ~ReservoirSampleStatisticFormat() override;

  private:
    ReservoirSampleStatisticFormat(const std::string& qualifierNameWithSeparator,
                                   Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutInput,
                                   std::function<std::string (const std::string&)> postProcessingData,
                                   std::function<std::string (const std::string&)> preProcessingData);

    const std::string sampleSizeFieldName;
    const std::string sampleDataFieldName;
    SchemaPtr sampleSchema;
};

}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_RESERVOIRSAMPLESTATISTICFORMAT_HPP_
