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

#ifndef NES_NES_COMMON_INCLUDE_UTIL_STATISTICCOLLECTORTYPE_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_STATISTICCOLLECTORTYPE_HPP_

namespace NES::Experimental::Statistics {
/**
 * @brief the types of StatisticCollectorFormats
 */
enum class StatisticCollectorType { COUNT_MIN, HYPER_LOG_LOG, DDSKETCH, RESERVOIR, UNDEFINED };
}// namespace NES::Experimental::Statistics
#endif//NES_NES_COMMON_INCLUDE_UTIL_STATISTICCOLLECTORTYPE_HPP_
