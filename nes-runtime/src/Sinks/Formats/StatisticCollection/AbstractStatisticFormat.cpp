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

#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Sinks/Formats/StatisticCollection/AbstractStatisticFormat.hpp>
#include <utility>

namespace NES::Statistic {

AbstractStatisticFormat::AbstractStatisticFormat(const Schema& schema,
                                                 Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                                 std::function<std::string(const std::string&)> postProcessingData,
                                                 std::function<std::string(const std::string&)> preProcessingData)
    : AbstractStatisticFormat(schema.getQualifierNameForSystemGeneratedFieldsWithSeparator(),
                              memoryLayout,
                              postProcessingData,
                              preProcessingData) {}

AbstractStatisticFormat::AbstractStatisticFormat(const std::string& qualifierNameWithSeparator,
                                                 Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout,
                                                 std::function<std::string(const std::string&)> postProcessingData,
                                                 std::function<std::string(const std::string&)> preProcessingData)
    : memoryLayout(std::move(memoryLayout)), startTsFieldName(qualifierNameWithSeparator + BASE_FIELD_NAME_START),
      endTsFieldName(qualifierNameWithSeparator + BASE_FIELD_NAME_END),
      statisticHashFieldName(qualifierNameWithSeparator + STATISTIC_HASH_FIELD_NAME),
      statisticTypeFieldName(qualifierNameWithSeparator + STATISTIC_TYPE_FIELD_NAME),
      observedTuplesFieldName(qualifierNameWithSeparator + OBSERVED_TUPLES_FIELD_NAME), postProcessingData(postProcessingData),
      preProcessingData(preProcessingData) {}

AbstractStatisticFormat::~AbstractStatisticFormat() = default;

}// namespace NES::Statistic