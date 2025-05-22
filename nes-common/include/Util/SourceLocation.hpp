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
#include <cstring>
/// The following provides a polyfill for the source location standard. On not supported platforms it will return an empty result.
#if __has_include(<source_location>)
    #include <source_location>
#elif __has_include(<experimental/source_location>)
    #include <experimental/source_location>

namespace std
{
using source_location = std::experimental::source_location;
}
#else
    #include <cstdint>
    #include <stdint.h>

namespace std
{
struct source_location
{
    static constexpr source_location current(
        const char* __file = __builtin_FILE(),
        const char* __func = __builtin_FUNCTION(),
        int __line = __builtin_LINE(),
        int __col = 0) noexcept
    {
        source_location __loc;
        __loc._M_file = __file;
        __loc._M_func = __func;
        __loc._M_line = __line;
        __loc._M_col = __col;
        return __loc;
    }

    constexpr source_location() noexcept : _M_file("unknown"), _M_func(_M_file), _M_line(0), _M_col(0) { }

    /// 14.1.3, source_location field access
    constexpr uint_least32_t line() const noexcept { return _M_line; }

    constexpr uint_least32_t column() const noexcept { return _M_col; }

    constexpr const char* file_name() const noexcept { return _M_file; }

    constexpr const char* function_name() const noexcept { return _M_func; }

private:
    const char* _M_file;
    const char* _M_func;
    uint_least32_t _M_line;
    uint_least32_t _M_col;
};
}
#endif
