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

#include <chrono>
#include <Util/Reflection/ReflectionCore.hpp>

namespace NES
{

/// Reflector/Unreflector for std::chrono::duration<Rep, Period>, serialized as the tick count.
/// The period is part of the static type, so it does not need to travel.
template <typename Rep, typename Period>
struct Reflector<std::chrono::duration<Rep, Period>>
{
    Reflected operator()(const std::chrono::duration<Rep, Period>& data) const { return reflect(data.count()); }
};

template <typename Rep, typename Period>
struct Unreflector<std::chrono::duration<Rep, Period>>
{
    std::chrono::duration<Rep, Period> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return std::chrono::duration<Rep, Period>{context.unreflect<Rep>(data)};
    }
};

}
