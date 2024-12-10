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

namespace NES::QueryCompilation
{
enum class CompilationStrategy : uint8_t
{
    /// Use fast compilation strategy, i.e., does not apply any optimizations and omits debug output.
    FAST,
    /// Creates debug output i.e., source code files and applies formatting. No code optimizations.
    DEBUG,
    /// Applies all compiler optimizations.
    OPTIMIZE,
    /// Applies all compiler optimizations and inlines proxy functions.
    PROXY_INLINING
};

enum class StreamJoinStrategy : uint8_t
{
    NESTED_LOOP_JOIN
};

enum class SliceCacheType : uint8_t
{
    DEFAULT,
    FIFO,
    LRU
};
}
