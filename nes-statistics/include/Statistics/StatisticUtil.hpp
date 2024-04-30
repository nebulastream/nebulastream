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

#ifndef NES_STATISTICS_INCLUDE_STATISTICS_STATISTICUTIL_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICS_STATISTICUTIL_HPP_
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::Statistic {

/**
 * @brief This class contains static utility methods for the statistics
 */
class StatisticUtil {
  public:
    /**
     * @brief Retrieves the H3 hash of the value
     * @param value
     * @param row
     * @param depth
     * @param numberOfBitsInKey
     * @return uint64_t
     */
    static uint64_t getH3HashValue(BasicValue& value, uint64_t row, uint64_t depth, uint64_t numberOfBitsInKey);

    /**
     * @brief Compares the field with the basic value.
     * @param leftField
     * @param value
     * @return True, if the field is equal to the value, false otherwise
     */
    static bool compareFieldWithBasicValue(const int8_t* leftField, const BasicValue& value);

    /**
     * @brief Creates a sample schema that drops all statistic fields from the input schema
     * @param inputSchema: Schema that contains statistic fields
     * @return SchemaPtr
     */
    static SchemaPtr createSampleSchema(const Schema& inputSchema);
};

}// namespace NES::Statistic

#endif// NES_STATISTICS_INCLUDE_STATISTICS_STATISTICUTIL_HPP_
