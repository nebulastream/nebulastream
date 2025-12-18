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
namespace detail
{
class BufferControlBlock;
}

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
    struct CombinedIndex
    {
        uint64_t index;
        uint64_t size;
    };

    class Index
    {
    public:
        /// Required for allowing VariableSizedAccess to access offset in VariableSizedAccess::getCombinedIdxOffset()
        friend class VariableSizedAccess;

        /// Required for allowing BufferControlBlock to access offset in BufferControlBlock::loadChildBuffer()
        friend class detail::BufferControlBlock;

        using Underlying = uint32_t;
        static constexpr auto UnderlyingBits = sizeof(Underlying) * 8;

        explicit Index(uint64_t index);
        static Index convertToIndex(CombinedIndex combinedIdxOffset);
        [[nodiscard]] Underlying getRawIndex() const;
        friend std::ostream& operator<<(std::ostream& os, const Index& index);
        friend std::strong_ordering operator<=>(const Index& lhs, const Index& rhs) = default;

        friend Underlying operator/(const Index& index, Underlying other);
        friend Underlying operator%(const Index& index, Underlying other);

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

        explicit Offset(uint64_t offset);

        static Offset convertToOffset(CombinedIndex combinedIdxOffset);
        [[nodiscard]] Underlying getRawOffset() const;

        friend std::ostream& operator<<(std::ostream& os, const Offset& offset);

        friend std::strong_ordering operator<=>(const Offset& lhs, const Offset& rhs) = default;

    private:
        Underlying offset;
    };

    class Size
    {
    public:
        using Underlying = size_t;
        static constexpr auto UnderlyingBits = sizeof(Underlying) * 8;

        explicit Size(uint64_t size);

        static Size convertToSize(CombinedIndex combinedIdx);
        [[nodiscard]] Underlying getRawSize() const;

        friend std::ostream& operator<<(std::ostream& os, const Size& size);

    private:
        Underlying size;
    };

private:
    /// The order of the variables are of utmost importance, as we are providing a nautilus::val<> wrapper.
    /// By calling the C++-runtime (via nautilus::invoke()), we do not call any conversion but rather "bit_cast" the CombinedIndex.
    /// Thus, we initialize the values of the offset and index indirectly and not via the constructor call.
    Offset offset;
    Index index;
    Size size;

public:
    VariableSizedAccess() : offset(0), index(0), size(0) { }

    explicit VariableSizedAccess(const CombinedIndex combinedIdxOffset)
        : offset(Offset::convertToOffset(combinedIdxOffset))
        , index(Index::convertToIndex(combinedIdxOffset))
        , size(Size::convertToSize(combinedIdxOffset))
    {
    }

    explicit VariableSizedAccess(const uint64_t combinedIdxOffset, const uint64_t size)
        : VariableSizedAccess(CombinedIndex{combinedIdxOffset, size})
    {
    }

    explicit VariableSizedAccess(const Index index, const Size size) : offset(0), index(index), size(size) { }

    explicit VariableSizedAccess(const Index index, const Offset offset, const Size size) : offset(offset), index(index), size(size) { }

    explicit VariableSizedAccess(const Offset offset, const Size size) : offset(offset), index(0), size(size) { }

    ~VariableSizedAccess() = default;

    [[nodiscard]] Index getIndex() const { return index; }

    [[nodiscard]] Offset getOffset() const { return offset; };

    [[nodiscard]] Size getSize() const { return size; };

    [[nodiscard]] CombinedIndex getCombinedIdxOffset() const
    {
        const uint64_t indexBitsCombined = static_cast<uint64_t>(index.index) << 32UL;
        const uint64_t offsetBitsCombined = offset.offset;
        return CombinedIndex{.index = indexBitsCombined | offsetBitsCombined, .size = size.getRawSize()};
    }

    friend std::ostream& operator<<(std::ostream& os, const VariableSizedAccess& obj)
    {
        return os << "VariableSizedAccess(index: " << obj.index << " offset: " << obj.offset << ")";
    }
};

static_assert(sizeof(VariableSizedAccess) == 16, "Underlying type must be 12 bytes");
static_assert(sizeof(VariableSizedAccess::CombinedIndex) == 16, "Underlying type must be 8 bytes (64 bits)");

}

FMT_OSTREAM(NES::VariableSizedAccess::Index);
FMT_OSTREAM(NES::VariableSizedAccess::Offset);
FMT_OSTREAM(NES::VariableSizedAccess);
