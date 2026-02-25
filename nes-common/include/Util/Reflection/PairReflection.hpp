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

template <typename T1, typename T2>
struct Reflector<std::pair<T1, T2>>
{
    Reflected operator()(const std::pair<T1, T2>& data) const
    {
        std::vector<rfl::Generic> arr;
        arr.push_back(*reflect(data.first));
        arr.push_back(*reflect(data.second));
        return Reflected{rfl::Generic::Array{std::move(arr)}};
    }
};

template <typename T1, typename T2>
struct Unreflector<std::pair<T1, T2>>
{
    std::pair<T1, T2> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return std::visit(
            [&context](const auto& value) -> std::pair<T1, T2>
            {
                using ValueType = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<ValueType, rfl::Generic::Array>)
                {
                    if (value.size() != 2)
                    {
                        throw CannotDeserialize("Expected array of size 2 for std::pair");
                    }
                    return std::make_pair(context.unreflect<T1>(Reflected{value[0]}), context.unreflect<T2>(Reflected{value[1]}));
                }
                throw CannotDeserialize("Expected array for std::pair");
            },
            data->variant());
    }
};

}
