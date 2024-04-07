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

#ifndef NES_OPTIMIZER_INCLUDE_UTIL_SYSPLANMETADATA_HPP_
#define NES_OPTIMIZER_INCLUDE_UTIL_SYSPLANMETADATA_HPP_

#include <Identifiers.hpp>

namespace NES::Optimizer {

/**
 * @brief Struct to store sub plan id and worker id that stores a sys generated query plan
 */
struct SysPlanMetaData {
    DecomposedQueryPlanId decomposedQueryPlanId;
    WorkerId workerId;
};
}// namespace NES::Optimizer

#endif // NES_OPTIMIZER_INCLUDE_UTIL_SYSPLANMETADATA_HPP_
