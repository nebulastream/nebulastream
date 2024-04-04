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

#ifndef NES_DISTRIBUTEDJOINOPTIMIZATIONMODE_HPP
#define NES_DISTRIBUTEDJOINOPTIMIZATIONMODE_HPP

#include <cstdint>

namespace NES::Optimizer {
enum class DistributedJoinOptimizationMode : uint8_t {
    NONE,  // distributed join optimization disabled
    MATRIX,// uses distributed matrix join approach
    NEMO   // uses distributed nemo join
};
}

#endif//NES_DISTRIBUTEDJOINOPTIMIZATIONMODE_HPP