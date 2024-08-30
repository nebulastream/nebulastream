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

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <nautilus/val.hpp>

namespace nautilus
{

namespace tracing
{
template <typename NES_STRONG>
constexpr Type to_type()
{
    using Underlying = typename NES_STRONG::Underlying;
    return to_type<Underlying>();
}
}

/// This class is a nautilus wrapper for our NESStrongType
template <typename T, typename Tag, T invalid, T initial>
class val<NES::NESStrongType<T, Tag, invalid, initial>>
{
public:
    using Underlying = typename NES::NESStrongType<T, Tag, invalid, initial>::Underlying;
    val<NES::NESStrongType<T, Tag, invalid, initial>>(const Underlying t) : value(t.value) { }
    val<NES::NESStrongType<T, Tag, invalid, initial>>(const val<Underlying> t) : value(t) { }
    val<NES::NESStrongType<T, Tag, invalid, initial>>(const NES::NESStrongType<T, Tag, invalid, initial> t) : value(t.getRawValue()) { }
    val<NES::NESStrongType<T, Tag, invalid, initial>>(nautilus::tracing::TypedValueRef typedValueRef) : value(typedValueRef) { }
    val<NES::NESStrongType<T, Tag, invalid, initial>>(const val<NES::NESStrongType<T, Tag, invalid, initial>>& other) : value(other.value)
    {
    }
    val<NES::NESStrongType<T, Tag, invalid, initial>> operator=(const val<NES::NESStrongType<T, Tag, invalid, initial>>& other)
    {
        value = other.value;
        return *this;
    }

    [[nodiscard]] friend bool operator<(const val& lh, const val& rh) noexcept { return lh.value < rh.value; }
    [[nodiscard]] friend bool operator<=(const val& lh, const val& rh) noexcept { return lh.value <= rh.value; }
    [[nodiscard]] friend bool operator>(const val& lh, const val& rh) noexcept { return lh.value > rh.value; }
    [[nodiscard]] friend bool operator>=(const val& lh, const val& rh) noexcept { return lh.value >= rh.value; }
    [[nodiscard]] friend bool operator==(const val& lh, const val& rh) noexcept { return lh.value == rh.value; }


    val<Underlying> value;
};
}
