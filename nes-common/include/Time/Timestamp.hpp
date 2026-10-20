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
#include <compare>
#include <cstdint>
#include <ostream>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

class Timestamp
{
public:
    using Underlying = uint64_t;
    static constexpr Underlying INITIAL_VALUE = 0;
    static constexpr Underlying INVALID_VALUE = UINT64_MAX;
    static constexpr Underlying INFINITE_VALUE = UINT64_MAX - 1;

    explicit constexpr Timestamp(const Underlying value) : value(value) { }

    /// The current wall-clock time in milliseconds since the Unix epoch.
    [[nodiscard]] static Timestamp nowInMilliseconds()
    {
        return Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    }

    /// Arithmetic saturates: infinity and invalid remain unchanged, and a finite timestamp never leaves [INITIAL_VALUE, INFINITE_VALUE].
    constexpr Timestamp operator+(const Underlying offset) const
    {
        if (value >= INFINITE_VALUE)
        {
            return *this;
        }
        return Timestamp(offset < INFINITE_VALUE - value ? value + offset : INFINITE_VALUE);
    }

    constexpr Timestamp operator-(const Underlying offset) const
    {
        if (value >= INFINITE_VALUE)
        {
            return *this;
        }
        return Timestamp(value > offset ? value - offset : INITIAL_VALUE);
    }

    constexpr Timestamp operator-(const Timestamp other) const { return *this - other.value; }

    friend std::ostream& operator<<(std::ostream& os, const Timestamp& timestamp) { return os << timestamp.value; }

    [[nodiscard]] Underlying getRawValue() const { return value; }

    friend constexpr std::strong_ordering operator<=>(const Timestamp& lhs, const Timestamp& rhs) = default;

private:
    Underlying value;
};

}

FMT_OSTREAM(NES::Timestamp);
