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

#ifndef NES_NES_COMMON_INCLUDE_STATISTICIDENTIFIERS_HPP_
#define NES_NES_COMMON_INCLUDE_STATISTICIDENTIFIERS_HPP_

#include <cstdint>
#include <string>

namespace NES::Statistic {

/* Names for the field an infrastructure data source is writing the value into. As we could have multiple
 * infrastructure metrics per node, we must have unique identifying field names.
 */
static const std::string INGESTION_RATE_FIELD_NAME = "INGESTION_RATE_FIELD_NAME";
static const std::string BUFFER_RATE_FIELD_NAME = "BUFFER_RATE_FIELD_NAME";
static const std::string INFRASTRUCTURE_BASE_LOGICAL_SOURCE_NAME = "INFRASTRUCTURE_LOGICAL_SOURCE_NAME_";

/** Names for the fields of the StatisticWindowOperatorNode*/
static const std::string OBSERVED_TUPLES_FIELD_NAME = "OBSERVED_TUPLES_SYNOPSIS_FIELD_NAME";
static const std::string BASE_FIELD_NAME_START = "BASE_FIELD_NAME_START";
static const std::string BASE_FIELD_NAME_END = "BASE_FIELD_NAME_END";
static const std::string STATISTIC_HASH_FIELD_NAME = "STATISTIC_HASH_FIELD_NAME";
static const std::string STATISTIC_DATA_FIELD_NAME = "STATISTIC_DATA_FIELD_NAME";
static const std::string STATISTIC_TYPE_FIELD_NAME = "STATISTIC_TYPE_FIELD_NAME";

/** Names for the fields of the synopsis */
static const std::string WIDTH_FIELD_NAME = "WIDTH_FIELD_NAME";
static const std::string DEPTH_FIELD_NAME = "DEPTH_FIELD_NAME";
}// namespace NES::Statistic

#endif//NES_NES_COMMON_INCLUDE_STATISTICIDENTIFIERS_HPP_
