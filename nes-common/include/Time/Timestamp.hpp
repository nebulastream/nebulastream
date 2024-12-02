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
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <nautilus/tracing/TypedValueRef.hpp>
#include <nautilus/tracing/Types.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_concepts.hpp>
#include <nautilus/function.hpp>


namespace NES::Runtime {
class Timestamp;
using SliceStart = Timestamp;
using SliceEnd = Timestamp;

class Timestamp {
public:
    using Underlying = uint64_t;
    static constexpr auto INITIAL_VALUE = 0;
    static constexpr auto INVALID_VALUE = UINT64_MAX;


    explicit Timestamp(const uint64_t value) : value(value)
    {
    }

    Timestamp operator+(const uint64_t offset) const
    {
        return Timestamp(value + offset);
    }

    Timestamp operator+=(const uint64_t offset) const
    {
        return Timestamp(value + offset);
    }

    Timestamp operator-(const uint64_t offset) const
    {
        return Timestamp(value - offset);
    }

    Timestamp operator-=(const uint64_t offset) const
    {
        return Timestamp(value - offset);
    }

    friend std::ostream& operator<<(std::ostream& os, const Timestamp& timestamp) {
        return os << timestamp.value;
    }

    uint64_t getRawValue() const
    {
        return value;
    }

    friend constexpr std::strong_ordering operator<=>(const Timestamp& lh, const Timestamp& rh) = default;

private:
    Underlying value;
};



}

template <typename T>
requires(std::is_base_of_v<NES::Runtime::Timestamp, T>)
struct fmt::formatter<T> : fmt::ostream_formatter
{
};