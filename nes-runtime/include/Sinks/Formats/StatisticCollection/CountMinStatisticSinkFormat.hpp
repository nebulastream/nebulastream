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

#ifndef NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_COUNTMINSTATISTICSINKFORMAT_HPP_
#define NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_COUNTMINSTATISTICSINKFORMAT_HPP_

#include <Sinks/Formats/StatisticCollection/AbstractStatisticSinkFormat.hpp>

namespace NES::Statistic {

/**
 * @brief StatisticSinkFormat that creates/builds CountMin-Sketches from a tuple buffer
 */
class CountMinStatisticSinkFormat : public AbstractStatisticSinkFormat {
  public:

    static AbstractStatisticSinkFormatPtr create(Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout);

    std::vector<std::pair<StatisticHash, StatisticPtr>> readStatisticsFromBuffer(Runtime::TupleBuffer& buffer) override;

    [[nodiscard]] std::string toString() const override;

    ~CountMinStatisticSinkFormat() override;

  private:
    CountMinStatisticSinkFormat(const std::string& qualifierNameWithSeparator,
                                Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout);

    const std::string widthFieldName;
    const std::string depthFieldName;
    const std::string countMinDataFieldName;
};

}// namespace NES::Statistic

#endif//NES_NES_RUNTIME_INCLUDE_SINKS_FORMATS_STATISTICCOLLECTION_COUNTMINSTATISTICSINKFORMAT_HPP_
