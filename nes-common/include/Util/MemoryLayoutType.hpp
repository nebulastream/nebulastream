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

#pragma once

#include <cstdint>

namespace NES
{

/// Memory layout definition, defining how the tuples in a tuple buffer are laid out.
enum class MemoryLayoutType : uint8_t
{
    /// Whole tuples are stored contiguously, one after the other (NSM).
    ROW_LAYOUT = 0,
    /// Each field is stored as its own contiguous column, on field value after the other (DMS).
    COLUMNAR_LAYOUT = 1
};

}
