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
#include <compare>
#include <cstdint>
#include <ostream>
#include <Util/Logger/Formatter.hpp>

namespace NES::Runtime
{

class Timestamp
{
public:
    using Underlying = uint64_t;
    static constexpr Underlying INITIAL_VALUE = 0;
    static constexpr Underlying INVALID_VALUE = UINT64_MAX;

    explicit constexpr Timestamp(const Underlying value) : value(value) { }

    Timestamp operator+(const Underlying offset) const { return Timestamp(value + offset); }

    Timestamp operator+=(const Underlying offset) const { return Timestamp(value + offset); }

    Timestamp operator-(const Underlying offset) const { return Timestamp(value - offset); }

    Timestamp operator-(const Timestamp other) const { return Timestamp(value - other.value); }

    Timestamp operator-=(const Underlying offset) const { return Timestamp(value - offset); }

    friend std::ostream& operator<<(std::ostream& os, const Timestamp& timestamp) { return os << timestamp.value; }

    [[nodiscard]] Underlying getRawValue() const { return value; }

    friend constexpr std::strong_ordering operator<=>(const Timestamp& lhs, const Timestamp& rhs) = default;

private:
    Underlying value;
};


}

FMT_OSTREAM(NES::Runtime::Timestamp);
