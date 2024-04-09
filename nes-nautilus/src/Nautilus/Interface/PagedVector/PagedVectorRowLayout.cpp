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

#include "PagedVectorRowLayout.h"

#include <cassert>

std::span<char> Entry::data()
{
    return page.data().subspan(offset, entrySize);
}

size_t& detail::EntryPage::varPageBase() const
{
    return *reinterpret_cast<size_t*>(_data.get());
}

std::span<char> detail::EntryPage::data()
{
    return {_data.get() + sizeof(size_t), ENTRY_PAGE_SIZE};
}

std::span<const char> detail::EntryPage::data() const
{
    return {_data.get() + sizeof(size_t), ENTRY_PAGE_SIZE};
}

std::span<char> detail::VarPage::data()
{
    return {(_data.get()), VAR_PAGE_SIZE};
}

std::span<const char> detail::VarPage::data() const
{
    return {(_data.get()), VAR_PAGE_SIZE};
}

Entry::Entry(detail::Badge<PagedVectorRowLayout>, detail::EntryPage& page, PagedVectorRowLayout& ref, size_t offset,
             size_t entry_size): page(page),
                                 ref(ref),
                                 offset(offset),
                                 entrySize(entry_size)
{
}

size_t Entry::storeVarSizedDataToOffset(std::span<const char> data, size_t offset)
{
    auto index = ref.storeVarSized(std::span(data.begin(), data.end()));
    auto relative = toRelativeIndex(index);
    *std::bit_cast<size_t*>(this->data().subspan(offset).data()) = data.size();
    *std::bit_cast<detail::RelativeVarPageIndex*>(this->data().subspan(offset + sizeof(size_t)).data()) = relative;
    return offset + sizeof(size_t) + sizeof(detail::RelativeVarPageIndex);
}

std::span<const char> Entry::loadVarSizedDataAtOffset(size_t offset) const
{
    auto size = *std::bit_cast<size_t*>(this->data().subspan(offset).data());
    auto relative = *std::bit_cast<detail::RelativeVarPageIndex*>(this->data().subspan(offset + sizeof(size_t)).data());
    auto absolute = toAbsoluteIndex(relative);
    return ref.loadVarSized(absolute, size);
}

std::span<const char> Entry::data() const
{
    return page.data().subspan(offset, entrySize);
}

detail::RelativeVarPageIndex Entry::toRelativeIndex(detail::AbsoluteVarPageIndex absolute) const
{
    assert(absolute.index <= SSIZE_MAX);
    return {((ssize_t)absolute.index) - ((ssize_t)page.varPageBase()), absolute.offset};
}

detail::AbsoluteVarPageIndex Entry::toAbsoluteIndex(detail::RelativeVarPageIndex relative) const
{
    return {relative.index + page.varPageBase(), relative.offset};
}

detail::AbsoluteVarPageIndex PagedVectorRowLayout::findOrAllocateVarSizePageForSize(size_t size)
{
    std::erase_if(varPageOccupation,
                  [this](const auto& po)
                  {
                      return detail::EntryPage::ENTRY_PAGE_SIZE - po.second < VAR_PAGE_SIZE_THRESHHOLD;
                  });

    for (auto& vp : varPageOccupation)
    {
        if (detail::VarPage::VAR_PAGE_SIZE - vp.second >= size)
        {
            auto offset = vp.second;
            vp.second += size;
            return {vp.first, offset};
        }
    }

    varSizedPages.emplace_back();
    varPageOccupation.emplace_back(varSizedPages.size() - 1, size);
    return {varSizedPages.size() - 1, 0};
}

size_t PagedVectorRowLayout::getEntrySize() const
{
    return entrySize;
}

size_t PagedVectorRowLayout::size() const
{
    return numberOfEntries;
}

PagedVectorRowLayout::PagedVectorRowLayout(size_t entrySize): entrySize(entrySize)
{
}

detail::AbsoluteVarPageIndex PagedVectorRowLayout::storeVarSized(std::span<const char> data)
{
    auto index = findOrAllocateVarSizePageForSize(data.size());
    auto dest_page_memory = varSizedPages[index.index].data()
                                                      .subspan(index.offset, data.size());
    assert(dest_page_memory.size() == data.size());
    std::copy(data.begin(), data.end(), dest_page_memory.begin());
    return index;
}

std::span<const char> PagedVectorRowLayout::loadVarSized(detail::AbsoluteVarPageIndex index, size_t size) const
{
    return varSizedPages[index.index].data().subspan(index.offset, size);
}

PagedVectorRowLayout::EntryIterator::EntryIterator(PagedVectorRowLayout& ref, size_t index): ref(ref),
    currentIndex(index), current_occupied(ref.pageOccupation.begin())
{
    if (ref.numberOfEntries == 0)
    {
        return;
    }

    if (current_occupied == ref.pageOccupation.end() || current_occupied->first != 0)
    {
        currentPageNumberOfEntries = detail::EntryPage::ENTRY_PAGE_SIZE / ref.entrySize;
    }
    else
    {
        currentPageNumberOfEntries = (current_occupied++)->second / ref.entrySize;
    }
}

PagedVectorRowLayout::EntryIterator::reference PagedVectorRowLayout::EntryIterator::operator*() const
{
    assert(currentPageOffset < currentPageNumberOfEntries);
    assert(currentPageIndex < ref.entryPages.size());
    assert(currentIndex < ref.entrySize);

    auto& page = ref.entryPages[currentPageIndex];

    return Entry({}, page, ref, currentPageOffset * ref.entrySize, ref.entrySize);
}

PagedVectorRowLayout::EntryIterator& PagedVectorRowLayout::EntryIterator::operator++()
{
    assert(currentIndex < ref.numberOfEntries);
    currentIndex++;
    if (++currentPageOffset < currentPageNumberOfEntries)
    {
        return *this;
    }
    if (++currentPageIndex < ref.entryPages.size())
    {
        currentPageOffset = 0;
        if (current_occupied == ref.pageOccupation.end() || current_occupied->first != currentPageIndex)
        {
            currentPageNumberOfEntries = detail::EntryPage::ENTRY_PAGE_SIZE / ref.entrySize;
        }
        else
        {
            currentPageNumberOfEntries = (current_occupied++)->second / ref.entrySize;
        }
        return *this;
    }
    assert(currentIndex == ref.numberOfEntries);
    return *this;
}

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::EntryIterator::operator++(int)
{
    EntryIterator tmp = *this;
    ++(*this);
    return tmp;
}

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::begin()
{
    return EntryIterator(*this, 0);
}

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::end()
{
    return EntryIterator(*this, numberOfEntries);
}

Entry PagedVectorRowLayout::allocateEntry()
{
    std::erase_if(pageOccupation,
                  [this](const auto& po) { return detail::EntryPage::ENTRY_PAGE_SIZE - po.second < entrySize; });

    if (pageOccupation.empty())
    {
        entryPages.emplace_back();
        entryPages.back().varPageBase() = entryPages.size() - 1;
        pageOccupation.emplace_back(entryPages.size() - 1, 0);
    }

    auto [pageIndex, pageOffset] = pageOccupation.front();
    pageOccupation.front().second += entrySize;
    numberOfEntries++;
    return Entry({}, entryPages[pageIndex], *this, pageOffset, entrySize);
}

void PagedVectorRowLayout::takePagesFrom(PagedVectorRowLayout&& other)
{
    assert(entrySize == other.entrySize);

    auto varPageOffset = varSizedPages.size();
    auto entryPageOffset = entryPages.size();
    entryPages.reserve(entryPages.size() + other.entryPages.size());
    varSizedPages.reserve(varSizedPages.size() + other.varSizedPages.size());

    for (auto& [index, _] : other.pageOccupation)
    {
        index += entryPageOffset;
    }

    //Update the Base Var Page Index
    for (auto& otherEntryPage : other.entryPages)
    {
        otherEntryPage.varPageBase() += varPageOffset;
    }

    for (auto& [index, _] : other.varPageOccupation)
    {
        index += varPageOffset;
    }

    std::ranges::move(other.entryPages, std::back_inserter(entryPages));
    std::ranges::move(other.varSizedPages, std::back_inserter(varSizedPages));
    std::ranges::move(other.pageOccupation, std::back_inserter(pageOccupation));
    std::ranges::move(other.varPageOccupation, std::back_inserter(varPageOccupation));
    numberOfEntries += other.numberOfEntries;
}

bool operator==(const PagedVectorRowLayout::EntryIterator& a, const PagedVectorRowLayout::EntryIterator& b)
{
    return a.currentIndex == b.currentIndex;
}

bool operator!=(const PagedVectorRowLayout::EntryIterator& a, const PagedVectorRowLayout::EntryIterator& b)
{
    return a.currentIndex != b.currentIndex;
}
