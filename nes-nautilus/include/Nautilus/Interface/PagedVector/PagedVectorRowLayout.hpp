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

#ifndef PAGEDVECTORROWLAYOUT_H
#define PAGEDVECTORROWLAYOUT_H
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <Util/Logger/Logger.hpp>
#include <deque>
#include <memory>
#include <span>
#include <vector>

#define TEST_PUBLIC public:
namespace NES::Runtime {
class Entry;
class PagedVectorRowLayout;

namespace detail {
template<typename T>
class Badge {
    friend T;
    Badge() = default;
};

struct EntryPage {
    using VarPageIndexType = uint32_t;
    static constexpr size_t ENTRY_PAGE_SIZE = 4192 - sizeof(VarPageIndexType);
    TupleBuffer page;

    explicit EntryPage(TupleBuffer&& page) : page(page) {
        NES_ASSERT(page.getBufferSize() == ENTRY_PAGE_SIZE, "Entry Page with unexpected size");
    }

    [[nodiscard]] VarPageIndexType& varPageBase();

    [[nodiscard]] std::span<char> data();
    [[nodiscard]] std::span<const char> data() const;
};

struct VarPage {
    static constexpr size_t VAR_PAGE_SIZE = 4192;
    TupleBuffer page;

    explicit VarPage(TupleBuffer&& page) : page(page) {
        NES_ASSERT(page.getBufferSize() == VAR_PAGE_SIZE, "Var Page with unexpected size");
    }

    [[nodiscard]] std::span<char> data();
    [[nodiscard]] std::span<const char> data() const;
};

struct alignas(uint64_t) AbsoluteVarPageIndex {
    uint32_t index;
    uint32_t offset;
};
static_assert(sizeof(AbsoluteVarPageIndex) == sizeof(uint64_t));

struct alignas(uint64_t) RelativeVarPageIndex {
    int32_t index;
    uint32_t offset;
};
static_assert(sizeof(AbsoluteVarPageIndex) == sizeof(uint64_t));
};// namespace detail

struct alignas(uint64_t) EntryRef {
  public:
    EntryRef(detail::Badge<Entry>, uint32_t entryPageIndex, uint32_t entryPageOffset);

    uint32_t entryPageIndex;
    uint32_t entryOffset;
};
static_assert(sizeof(EntryRef) == sizeof(uint64_t));

class Entry {
  public:
    Entry(detail::Badge<PagedVectorRowLayout>,
          uint32_t pageIndex,
          detail::EntryPage& page,
          PagedVectorRowLayout& ref,
          size_t offset,
          size_t entrySize);

    void* storeVarSizedDataToOffset(std::span<const char> data, void* offset);
    [[nodiscard]] std::pair<Nautilus::TextValue*, void*> loadVarSizedDataAtOffset(void* offset) const;

    [[nodiscard]] std::span<char> data();
    [[nodiscard]] std::span<const char> data() const;
    [[nodiscard]] EntryRef getRef() const { return EntryRef({}, pageIndex, offset); }

  private:
    [[nodiscard]] detail::RelativeVarPageIndex toRelativeIndex(detail::AbsoluteVarPageIndex absolute) const;
    [[nodiscard]] detail::AbsoluteVarPageIndex toAbsoluteIndex(detail::RelativeVarPageIndex relative) const;
    TEST_PUBLIC
    uint32_t pageIndex;
    detail::EntryPage& page;
    PagedVectorRowLayout& ref;
    size_t offset;
    size_t entrySize;
};

class PagedVectorRowLayout {
    constexpr static size_t VAR_PAGE_SIZE_THRESHHOLD = 100;

    detail::AbsoluteVarPageIndex findOrAllocateVarSizePageForSize(size_t size);
    TEST_PUBLIC
    BufferManagerPtr bufferManager;
    uint32_t entrySize;
    size_t numberOfEntries = 0;
    std::deque<std::pair<uint32_t, uint32_t>> pageOccupation;
    std::deque<std::pair<uint32_t, uint32_t>> varPageOccupation;
    std::vector<detail::EntryPage> entryPages;
    std::vector<detail::VarPage> varSizedPages;

  public:
    [[nodiscard]] size_t getEntrySize() const;
    [[nodiscard]] size_t size() const;

    Entry getEntry(EntryRef ref);

    explicit PagedVectorRowLayout(BufferManagerPtr bufferManager, size_t entrySize, uint64_t entryPageSize);

    [[nodiscard]] detail::AbsoluteVarPageIndex storeVarSized(std::span<const char> data);

    [[nodiscard]] std::span<const char> loadVarSized(detail::AbsoluteVarPageIndex index, size_t size) const;

    class EntryIterator {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = size_t;
        using value_type = Entry;
        using pointer = Entry;  // or also value_type*
        using reference = Entry;// or also value_type&

        PagedVectorRowLayout& ref;
        uint32_t currentPageIndex = 0;
        uint32_t currentPageOffset = 0;
        uint32_t currentPageNumberOfEntries = 0;
        uint32_t currentIndex = 0;
        std::deque<std::pair<uint32_t, uint32_t>>::const_iterator current_occupied;

      public:
        explicit EntryIterator(PagedVectorRowLayout& ref, size_t index);

        reference operator*() const;

        // Prefix increment
        EntryIterator& operator++();

        // Postfix increment
        EntryIterator operator++(int);

        friend bool operator==(const EntryIterator& a, const EntryIterator& b);
        friend bool operator!=(const EntryIterator& a, const EntryIterator& b);
    };

    EntryIterator begin();
    EntryIterator* beginAlloc();
    EntryIterator end();
    EntryIterator* endAlloc();
    Entry allocateEntry();

    void takePagesFrom(PagedVectorRowLayout&& other);
};
}// namespace NES::Runtime
#endif//PAGEDVECTORROWLAYOUT_H
