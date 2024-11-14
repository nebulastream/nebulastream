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

#include <Identifiers/NESStrongType.hpp>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>

namespace nautilus
{

namespace tracing
{
template <typename T, typename Tag, T Invalid, T Initial>
constexpr Type to_type()
{
    return to_type<T>();
}
}


namespace details
{
template <typename T, typename Tag, T Invalid, T Initial>
T getRawValue(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val);

template <typename T, typename Tag, T Invalid, T Initial>
tracing::TypedValueRef getState(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val);
}

/// This class is a nautilus wrapper for our NESStrongType
template <typename T, typename Tag, T Invalid, T Initial>
class val<NES::NESStrongType<T, Tag, Invalid, Initial>>
{
public:
    using Underlying = typename NES::NESStrongType<T, Tag, Invalid, Initial>::Underlying;
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const Underlying type) : value(type) { }
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const val<Underlying> type) : value(type) { }
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const NES::NESStrongType<T, Tag, Invalid, Initial> type) : value(type.getRawValue())
    {
    }
    explicit val<NES::NESStrongType<T, Tag, Invalid, Initial>>(nautilus::tracing::TypedValueRef typedValueRef) : value(typedValueRef) { }
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& other) : value(other.value)
    {
    }
    val<NES::NESStrongType<T, Tag, Invalid, Initial>>& operator=(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& other)
    {
        value = other.value;
        return *this;
    }

    [[nodiscard]] friend bool operator<(const val& lh, const val& rh) noexcept { return lh.value < rh.value; }
    [[nodiscard]] friend bool operator<=(const val& lh, const val& rh) noexcept { return lh.value <= rh.value; }
    [[nodiscard]] friend bool operator>(const val& lh, const val& rh) noexcept { return lh.value > rh.value; }
    [[nodiscard]] friend bool operator>=(const val& lh, const val& rh) noexcept { return lh.value >= rh.value; }
    [[nodiscard]] friend bool operator==(const val& lh, const val& rh) noexcept { return lh.value == rh.value; }

    friend T details::getRawValue(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val);
    friend tracing::TypedValueRef details::getState(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val);

    val<Underlying> value;
};

namespace details
{
template <typename T, typename Tag, T Invalid, T Initial>
T getRawValue(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val)
{
    return details::getRawValue(val.value);
}

template <typename T, typename Tag, T Invalid, T Initial>
tracing::TypedValueRef getState(const val<NES::NESStrongType<T, Tag, Invalid, Initial>>& val)
{
    return details::getState(val.value);
}
}

}
