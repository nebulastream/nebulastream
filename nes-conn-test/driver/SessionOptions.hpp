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
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>

#include <ErrorHandling.hpp>

namespace NES::ConnTest
{

/// A numeric session option whose value is always within [Min, Max].
/// The constructor throws for any other value, and the type has no arithmetic and no setter,
/// so a value that is computed from others is checked when it becomes an option again.
/// A constant outside the bounds does not compile, because the throw cannot be evaluated at compile time.
/// `Tag::NAME` says in an error which option it is, e.g. `the buffer size must be between 1 and 4294967295, got 0`.
template <typename Tag, std::unsigned_integral T, T Min, T Max = std::numeric_limits<T>::max()>
class BoundedOption
{
public:
    /// The value's type may be wider than the option's, so that a value which the option cannot hold fails here, not at a cast.
    template <std::integral Value>
    constexpr explicit BoundedOption(const Value value) : value(static_cast<T>(value))
    {
        if (std::cmp_less(value, Min) or std::cmp_greater(value, Max))
        {
            throw InvalidConfigParameter("{} must be between {} and {}, got {}", Tag::NAME, Min, Max, value);
        }
    }

    [[nodiscard]] constexpr T getRawValue() const { return value; }

private:
    T value;
};

struct BufferSizeTag
{
    static constexpr std::string_view NAME = "the buffer size";
};

struct BufferCountTag
{
    static constexpr std::string_view NAME = "the buffer count";
};

struct WorkerThreadsTag
{
    static constexpr std::string_view NAME = "the number of worker threads";
};

/// The pool takes 32-bit sizes and counts, and a size of 0 would divide by zero when the sink splits its input into buffers.
using BufferSize = BoundedOption<BufferSizeTag, std::uint32_t, 1>;
using BufferCount = BoundedOption<BufferCountTag, std::uint32_t, 1>;
using WorkerThreads = BoundedOption<WorkerThreadsTag, std::uint32_t, 1>;

}
