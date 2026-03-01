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

#include <type_traits>
#include <utility>
#include <vector>
#include <Util/Reflection/ReflectionCore.hpp>
#include <rfl/Generic.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

template <typename T>
struct Reflector<std::vector<T>>
{
    Reflected operator()(const std::vector<T>& data) const
    {
        std::vector<rfl::Generic> result;
        result.reserve(data.size());
        for (const auto& element : data)
        {
            auto reflected = reflect(element);
            result.push_back(reflected);
        }
        return Reflected{rfl::Generic::Array{std::move(result)}};
    }
};

template <typename T>
struct Unreflector<std::vector<T>>
{
    std::vector<T> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return std::visit(
            [&context](const auto& value) -> std::vector<T>
            {
                using ValueType = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<ValueType, rfl::Generic::Array>)
                {
                    std::vector<T> result;
                    for (const auto& element : value)
                    {
                        result.push_back(context.unreflect<T>(Reflected{element}));
                    }
                    return result;
                }
                throw CannotDeserialize("Expected array for vector type {}", typeid(T).name());
            },
            data->variant());
    }
};

}
