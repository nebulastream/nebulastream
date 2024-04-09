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
#include <span>
#include <memory>
#include <vector>
#include <deque>
class PagedVectorRowLayout;

#define TEST_PUBLIC public:

namespace detail
{
    template <typename T>
    class Badge
    {
        friend T;
        Badge() = default;
    };

    struct EntryPage
    {
        static constexpr size_t ENTRY_PAGE_SIZE = 4192 - sizeof(size_t);
        std::unique_ptr<char[]> _data = std::make_unique<char[]>(ENTRY_PAGE_SIZE + sizeof(size_t));

        [[nodiscard]] size_t& varPageBase() const;

        [[nodiscard]] std::span<char> data();
        [[nodiscard]] std::span<const char> data() const;
    };

    struct VarPage
    {
        static constexpr size_t VAR_PAGE_SIZE = 4192;
        std::unique_ptr<char[]> _data = std::make_unique<char[]>(VAR_PAGE_SIZE);

        [[nodiscard]] std::span<char> data();
        [[nodiscard]] std::span<const char> data() const;
    };

    struct AbsoluteVarPageIndex
    {
        size_t index;
        size_t offset;
    };

    struct RelativeVarPageIndex
    {
        ssize_t index;
        size_t offset;
    };
};

class Entry
{
public:
    Entry(detail::Badge<PagedVectorRowLayout>, detail::EntryPage& page, PagedVectorRowLayout& ref, size_t offset,
          size_t entry_size);

    size_t storeVarSizedDataToOffset(std::span<const char> data, size_t offset);
    [[nodiscard]] std::span<const char> loadVarSizedDataAtOffset(size_t offset) const;

    [[nodiscard]] std::span<char> data();
    [[nodiscard]] std::span<const char> data() const;

private:
    [[nodiscard]] detail::RelativeVarPageIndex toRelativeIndex(detail::AbsoluteVarPageIndex absolute) const;
    [[nodiscard]] detail::AbsoluteVarPageIndex toAbsoluteIndex(detail::RelativeVarPageIndex relative) const;
TEST_PUBLIC
    detail::EntryPage& page;
    PagedVectorRowLayout& ref;
    size_t offset;
    size_t entrySize;
};

class PagedVectorRowLayout
{
    constexpr static size_t VAR_PAGE_SIZE_THRESHHOLD = 100;

    detail::AbsoluteVarPageIndex findOrAllocateVarSizePageForSize(size_t size);
TEST_PUBLIC
    size_t entrySize;
    size_t numberOfEntries = 0;
    std::deque<std::pair<size_t, size_t>> pageOccupation;
    std::deque<std::pair<size_t, size_t>> varPageOccupation;
    std::vector<detail::EntryPage> entryPages;
    std::vector<detail::VarPage> varSizedPages;

public:
    [[nodiscard]] size_t getEntrySize() const;
    [[nodiscard]] size_t size() const;

    explicit PagedVectorRowLayout(size_t entrySize);


    [[nodiscard]] detail::AbsoluteVarPageIndex storeVarSized(std::span<const char> data);

    [[nodiscard]] std::span<const char> loadVarSized(detail::AbsoluteVarPageIndex index, size_t size) const;

    class EntryIterator
    {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = size_t;
        using value_type = Entry;
        using pointer = Entry; // or also value_type*
        using reference = Entry; // or also value_type&

        PagedVectorRowLayout& ref;
        size_t currentPageIndex = 0;
        size_t currentPageOffset = 0;
        size_t currentPageNumberOfEntries = 0;
        size_t currentIndex = 0;
        std::deque<std::pair<size_t, size_t>>::const_iterator current_occupied;

    public:
        explicit EntryIterator(PagedVectorRowLayout& ref, size_t index);

        reference operator*() const;

        // Prefix increment
        EntryIterator& operator++();

        // Postfix increment
        EntryIterator operator++(int);

        friend bool operator==(const EntryIterator& a, const EntryIterator& b);;

        friend bool operator!=(const EntryIterator& a, const EntryIterator& b);;
    };

    EntryIterator begin();

    EntryIterator end();

    Entry allocateEntry();

    void takePagesFrom(PagedVectorRowLayout&& other);
};


#endif //PAGEDVECTORROWLAYOUT_H
