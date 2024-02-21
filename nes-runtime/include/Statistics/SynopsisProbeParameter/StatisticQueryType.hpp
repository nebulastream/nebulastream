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

#ifndef NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_STATISTICQUERYTYPE_HPP_
#define NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_STATISTICQUERYTYPE_HPP_

namespace NES::Experimental::Statistics {
/**
 * @brief defines the different types of statistical queries
 */
enum class StatisticQueryType { POINT_QUERY, RANGE_QUERY, COUNT_DISTINCT };
}

#endif//NES_NES_RUNTIME_SRC_STATISTICS_STATISTICMANAGER_SYNOPSISPROBEPARAMETER_STATISTICQUERYTYPE_HPP_
