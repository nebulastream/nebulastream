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

#ifndef NES_MEMORYLAYOUTPOLICY_HPP
#define NES_MEMORYLAYOUTPOLICY_HPP

#include <cstdint>

namespace NES::Optimizer {
enum class MemoryLayoutPolicy : uint8_t {
    // Enforces a row layout for all operators within a query.
    FORCE_ROW_LAYOUT,
    // Enforces a columnar layout for all operators within a query.
    FORCE_COLUMN_LAYOUT
};
}

#endif//NES_MEMORYLAYOUTPOLICY_HPP
