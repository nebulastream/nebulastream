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

#ifndef UNIKERNEL_PROXY_COMMON_H
#define UNIKERNEL_PROXY_COMMON_H
#include <Util/Logger/Logger.hpp>
#include <proxy/fmt.hpp>
#include <ranges>
#include <string_view>

#ifdef UNIKERNEL_TRACE_PROXY_FUNCTIONS
#define TRACE_PROXY_FUNCTION(...) NES_DEBUG("{}({})", __func__, print_args(#__VA_ARGS__, __VA_ARGS__))
#define TRACE_PROXY_FUNCTION_NO_ARG NES_DEBUG("{}", __func__)
template<typename... Ts>
std::string print_args(std::string fields, const Ts&... t) {
    std::stringstream ss;
    std::vector<std::string_view> field_names;
    auto splits = std::ranges::views::split(std::string_view(fields), std::string_view(","));
    std::transform(splits.begin(), splits.end(), std::back_inserter(field_names), [](auto&& rng) -> std::string_view {
        auto str = std::string_view(rng.begin(), rng.end());
        size_t first = str.find_first_not_of(" \t\n\r");
        size_t last = str.find_last_not_of(" \t\n\r");
        if (first == std::string_view::npos) {
            return "";// Entire string is whitespace
        }
        return str.substr(first, last - first + 1);
    });

    std::size_t n{0};
    ((ss << field_names[n] << ": " << fmt::format("{}", t) << (++n != sizeof...(Ts) ? ", " : "")), ...);
    return ss.str();
}
#else
#define TRACE_PROXY_FUNCTION(...)
#define TRACE_PROXY_FUNCTION_NO_ARG
#endif
#define PROXY_FN [[maybe_unused]] static inline

#endif//UNIKERNEL_PROXY_COMMON_H
