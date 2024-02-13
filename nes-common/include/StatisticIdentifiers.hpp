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
}// namespace NES::Statistic

#endif//NES_NES_COMMON_INCLUDE_STATISTICIDENTIFIERS_HPP_
