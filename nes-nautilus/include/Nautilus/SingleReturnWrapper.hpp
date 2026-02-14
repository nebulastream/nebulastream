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

#include <concepts>
#include <functional>
#include <optional>
#include <type_traits>

namespace NES
{

template <typename Fn, typename... Args>
requires(std::invocable<Fn, Args...>)
auto singleReturnWrapper(Fn&& func, Args&&... args)
{
    using ResultT = std::invoke_result_t<Fn, Args...>;
    std::optional<ResultT> result;
    result.emplace(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...));
    return std::move(*result);
}

}

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define SINGLE_RETURN_WRAPPER(body) NES::singleReturnWrapper([&]() -> decltype(auto) body)
// NOLINTEND(cppcoreguidelines-macro-usage)
