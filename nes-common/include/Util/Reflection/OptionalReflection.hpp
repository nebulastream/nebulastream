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

#include <optional>
#include <Util/Reflection/ReflectionCore.hpp>

namespace NES
{

template <typename T>
struct Reflector<std::optional<T>>
{
    Reflected operator()(const std::optional<T>& data) const
    {
        if (data.has_value())
        {
            return reflect(data.value());
        }
        return Reflected{std::nullopt};
    }
};

template <typename T>
struct Unreflector<std::optional<T>>
{
    std::optional<T> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        if (data.isEmpty() || data->is_null())
        {
            return std::nullopt;
        }
        return context.unreflect<T>(data);
    }
};

}
