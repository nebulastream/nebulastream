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

#include <cstdint>
#include <ErrorHandling.hpp>

namespace NES
{

/// @brief This class is a helper class for accessing variable sized data in a tuple buffer.
/// We store variable sized data as child buffer. To reference one variable sized data object, we require an index to the child buffer and
/// an offset in the child buffer locating the start of the variable sized data object.
///
/// We use 64 bits to store the child index and offset for accessing the correct variable sized data.
/// We use the upper 20 bits for the childIndex and the lower 44 bits for the childBufferOffset
/// This allows us to have 1048576 child buffer and having a maximum child buffer size of 16384 GB
/// (unless we have only one var sized object per child)
class VariableSizedAccess
{
public:
    static constexpr auto NUMBER_OF_INDEX_BITS = 20UL;
    static constexpr auto NUMBER_OF_OFFSET_BITS = 44UL;
    using Underlying = uint64_t;
    static_assert(sizeof(Underlying) == (NUMBER_OF_INDEX_BITS + NUMBER_OF_OFFSET_BITS) / 8, "Underlying type must be 64 bits");

private:
    Underlying index : NUMBER_OF_INDEX_BITS;
    Underlying offset : NUMBER_OF_OFFSET_BITS;

public:
    VariableSizedAccess() : index(0), offset(0) { }

    explicit VariableSizedAccess(const uint64_t combinedIdxOffset) { *this = std::bit_cast<VariableSizedAccess>(combinedIdxOffset); }

    ~VariableSizedAccess() = default;

    VariableSizedAccess& setIndex(const uint64_t index)
    {
        PRECONDITION(
            index < (1UL << VariableSizedAccess::NUMBER_OF_INDEX_BITS),
            "Currently we only support {} child buffers",
            (1UL << VariableSizedAccess::NUMBER_OF_INDEX_BITS));
        this->index = index;
        return *this;
    }

    VariableSizedAccess& setOffset(const uint64_t offset)
    {
        PRECONDITION(
            offset < (1UL << VariableSizedAccess::NUMBER_OF_OFFSET_BITS),
            "Currently we only support {} ({}bit) offsets",
            (1UL << VariableSizedAccess::NUMBER_OF_INDEX_BITS),
            VariableSizedAccess::NUMBER_OF_INDEX_BITS);
        this->offset = offset;
        return *this;
    }

    [[nodiscard]] uint64_t getIndex() const { return index; }

    [[nodiscard]] uint64_t getOffset() const { return offset; };

    [[nodiscard]] uint64_t getCombinedIdxOffset() const { return std::bit_cast<uint64_t>(*this); }
};
}
