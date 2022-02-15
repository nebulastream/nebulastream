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
#ifndef NES_NES_CORE_INCLUDE_UTIL_SOURCELOCATION_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_SOURCELOCATION_HPP_

// The following provides a polyfill for the source location standard. On not supported platforms it will return a empty result.
#if __cplusplus > 201703L && __has_builtin(__builtin_source_location) && __has_include(<source_location>)
#include <source_location>
#elif __has_include(<experimental/source_location>)
#include <experimental/source_location>
namespace std {
using source_location = std::experimental::source_location;
}

#else

namespace std {
struct source_location {
    // 14.1.3, source_location field access
    constexpr uint_least32_t line() const noexcept { return 0; }
    constexpr uint_least32_t column() const noexcept { return 0; }
    constexpr const char* file_name() const noexcept { return ""; }
    constexpr const char* function_name() const noexcept { return ""; }
    static constexpr source_location current() noexcept { return source_location(); }
};
}// namespace std
#endif

#endif//NES_NES_CORE_INCLUDE_UTIL_SOURCELOCATION_HPP_
