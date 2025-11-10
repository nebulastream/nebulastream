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
#include <ErrorHandling.hpp>

namespace NES
{

/// @brief This class is a helper class for accessing variable sized data in a tuple buffer.
/// We store variable sized data as child buffer. To reference one variable sized data object, we require an index to the child buffer and
/// an offset in the child buffer locating the start of the variable sized data object.
///
/// We use 64 bits to store the child index and offset for accessing the correct variable sized data.
/// We use the upper 32 bits for the childIndex and the lower 32 bits for the childBufferOffset
/// This allows us to have 4 million child buffer and having a maximum child buffer size of 4 GB
/// (unless we have only one var sized object per child)
class VariableSizedAccess
{
public:
    class Index
    {
    public:
        /// Required for allowing VariableSizedAccess to access offset in VariableSizedAccess::getCombinedIdxOffset()
        friend class VariableSizedAccess;

        using Underlying = uint32_t;
        static constexpr auto UnderlyingBits = sizeof(Underlying) * 8;

        explicit Index(const uint64_t index) : index(index)
        {
            PRECONDITION(index < (1UL << UnderlyingBits), "Currently we only support {} child buffers", (1UL << UnderlyingBits));
        }

        static Index convertToIndex(const uint64_t combinedIdxOffset) { return Index{static_cast<uint32_t>(combinedIdxOffset >> 32UL)}; }

        friend std::ostream& operator<<(std::ostream& os, const Index& index) { return os << index.index; }

        friend std::strong_ordering operator<=>(const Index& lhs, const Index& rhs) = default;

        [[nodiscard]] Underlying getRawIndex() const { return index; }

    private:
        Underlying index;
    };

    class Offset
    {
    public:
        /// Required for allowing VariableSizedAccess to access offset in VariableSizedAccess::getCombinedIdxOffset()
        friend class VariableSizedAccess;
        using Underlying = uint32_t;
        static constexpr auto UnderlyingBits = sizeof(Underlying) * 8;

        explicit Offset(const uint64_t offset) : offset(offset)
        {
            PRECONDITION(
                offset < (1UL << UnderlyingBits), "Currently we only support {} ({}bit) offsets", (1UL << UnderlyingBits), UnderlyingBits);
        }

        static Offset convertToOffset(const uint64_t combinedIdxOffset)
        {
            return Offset{static_cast<uint32_t>(combinedIdxOffset & 0xffffffffUL)};
        }

        friend std::ostream& operator<<(std::ostream& os, const Offset& offset) { return os << offset.offset; }

        friend std::strong_ordering operator<=>(const Offset& lhs, const Offset& rhs) = default;

        /// We allow to directly add the offset to a pointer. Thus, we do not need to return the
        friend int8_t* operator+(const Offset& offset, int8_t* pointer) { return pointer + offset.offset; }

        friend int8_t* operator+(int8_t* pointer, const Offset& offset) { return pointer + offset.offset; }

    private:
        Underlying offset;
    };

private:
    /// The order of the variables are of utmost importance, as we are providing a nautilus::val<> wrapper.
    /// By calling the C++-runtime (via nautilus::invoke()), we do not call any conversion but rather "bit_cast" the CombinedIndex.
    /// Thus, we initialize the values of the offset and index indirectly and not via the constructor call.
    Offset offset;
    Index index;


public:
    using CombinedIndex = uint64_t;

    explicit VariableSizedAccess(const CombinedIndex combinedIdxOffset)
        : offset(Offset::convertToOffset(combinedIdxOffset)), index(Index::convertToIndex(combinedIdxOffset))
    {
    }

    explicit VariableSizedAccess(const Index index) : offset(0), index(index) { }

    explicit VariableSizedAccess(const Index index, const Offset offset) : offset(offset), index(index) { }

    explicit VariableSizedAccess(const Offset offset) : offset(offset), index(0) { }

    ~VariableSizedAccess() = default;

    [[nodiscard]] Index getIndex() const { return index; }

    [[nodiscard]] Offset getOffset() const { return offset; };

    [[nodiscard]] CombinedIndex getCombinedIdxOffset() const
    {
        const uint64_t indexBitsCombined = static_cast<uint64_t>(index.index) << 32UL;
        const uint64_t offsetBitsCombined = offset.offset;
        return indexBitsCombined | offsetBitsCombined;
    }

    friend std::ostream& operator<<(std::ostream& os, const VariableSizedAccess& obj)
    {
        return os << "VariableSizedAccess(index: " << obj.index << " offset: " << obj.offset << ")";
    }
};

static_assert(sizeof(VariableSizedAccess) == 8, "Underlying type must be 8 bytes (64 bits)");
static_assert(sizeof(VariableSizedAccess::CombinedIndex) == 8, "Underlying type must be 8 bytes (64 bits)");

}

FMT_OSTREAM(NES::VariableSizedAccess::Index);
FMT_OSTREAM(NES::VariableSizedAccess::Offset);
FMT_OSTREAM(NES::VariableSizedAccess);
