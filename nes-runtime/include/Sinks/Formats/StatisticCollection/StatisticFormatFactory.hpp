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

#ifndef NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_STATISTICFORMATFACTORY_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_STATISTICFORMATFACTORY_HPP_

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
namespace NES::Statistic {

/**
 * @brief Factory for creating StatisticFormat
 */
class StatisticFormatFactory {
  public:
    /**
     * @brief Creates the corresponding StatisticFormat for the type.
     * @param schema
     * @param bufferSize
     * @param type
     * @return StatisticFormatPtr
     */
    static StatisticFormatPtr
    createFromSchema(SchemaPtr schema, uint64_t bufferSize, StatisticSynopsisType type, StatisticDataCodec sinkDataCodec);

  private:
    /**
     * @brief Creates a CountMinStatisticFormat for this memoryLayout
     * @param memoryLayout
     * @param postProcessingData
     * @param preProcessingData
     * @return StatisticFormatPtr
     */
    static StatisticFormatPtr createCountMinFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                   std::function<std::string(const std::string&)> postProcessingData,
                                                   std::function<std::string(const std::string&)> preProcessingData);

    /**
     * @brief Creates a HyperLogLogStatisticFormat for this memoryLayout
     * @param memoryLayout
     * @param postProcessingData
     * @param preProcessingData
     * @return StatisticFormatPtr
     */
    static StatisticFormatPtr createHyperLogLogFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                      std::function<std::string(const std::string&)> postProcessingData,
                                                      std::function<std::string(const std::string&)> preProcessingData);

    /**
     * @brief Creates a ReservoirSampleStatisticFormat for this memoryLayout
     * @param memoryLayout
     * @param postProcessingData
     * @param preProcessingData
     * @return StatisticFormatPtr
     */
    static StatisticFormatPtr createReservoirSampleFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                                  std::function<std::string (const std::string&)> postProcessingData,
                                                                  std::function<std::string (const std::string&)> preProcessingData);
    
    /**
     * @brief Creates a DDSketchStatisticFormat for this memoryLayout
     * @param memoryLayout 
     * @param postProcessingData 
     * @param preProcessingData 
     * @return StatisticFormatPtr
     */
    static StatisticFormatPtr createDDSketchFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                   std::function<std::string(const std::string&)> postProcessingData,
                                                   std::function<std::string(const std::string&)> preProcessingData);
};

}// namespace NES::Statistic

#endif// NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_STATISTICFORMATFACTORY_HPP_
