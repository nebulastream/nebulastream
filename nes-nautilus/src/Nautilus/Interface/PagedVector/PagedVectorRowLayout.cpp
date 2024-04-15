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

#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRowLayout.hpp>

#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime {
std::span<char> Entry::data() { return page.data().subspan(offset, entrySize); }

detail::EntryPage::VarPageIndexType& detail::EntryPage::varPageBase() { return *page.getBuffer<VarPageIndexType>(); }

std::span<char> detail::EntryPage::data() { return {page.getBuffer<char>() + sizeof(VarPageIndexType), page.getBufferSize()}; }

std::span<const char> detail::EntryPage::data() const {
    return {page.getBuffer<const char>() + sizeof(VarPageIndexType), page.getBufferSize()};
}

std::span<char> detail::VarPage::data() { return {page.getBuffer<char>(), page.getBufferSize()}; }

std::span<const char> detail::VarPage::data() const { return {page.getBuffer<const char>(), page.getBufferSize()}; }

EntryRef::EntryRef(detail::Badge<Entry>, uint32_t entryPageIndex, uint32_t entryPageOffset)
    : entryPageIndex(entryPageIndex), entryOffset(entryPageOffset) {}

Entry::Entry(detail::Badge<PagedVectorRowLayout>,
             uint32_t pageIndex,
             detail::EntryPage& page,
             PagedVectorRowLayout& ref,
             size_t offset,
             size_t entrySize)
    : pageIndex(pageIndex), page(page), ref(ref), offset(offset), entrySize(entrySize) {}

void* Entry::storeVarSizedDataToOffset(std::span<const char> data, void* offset_ptr) {
    auto offset = std::distance(this->data().data(), static_cast<char*>(offset_ptr));
    NES_ASSERT(offset >= 0 && static_cast<size_t>(offset) < entrySize, "Wyld Offset");
    auto index = ref.storeVarSized(std::span(data.begin(), data.end()));
    auto relative = toRelativeIndex(index);
    *std::bit_cast<size_t*>(this->data().subspan(offset).data()) = data.size();
    *std::bit_cast<detail::RelativeVarPageIndex*>(this->data().subspan(offset + sizeof(size_t)).data()) = relative;
    return static_cast<char*>(offset_ptr) + sizeof(size_t) + sizeof(detail::RelativeVarPageIndex);
}

std::pair<Nautilus::TextValue*, void*> Entry::loadVarSizedDataAtOffset(void* offset_ptr) const {
    auto offset = std::distance(this->data().data(), static_cast<const char*>(offset_ptr));
    NES_ASSERT(offset >= 0 && static_cast<size_t>(offset) < entrySize, "Wyld Offset");
    auto size = *std::bit_cast<size_t*>(this->data().subspan(offset).data());
    auto relative = *std::bit_cast<detail::RelativeVarPageIndex*>(this->data().subspan(offset + sizeof(size_t)).data());
    auto absolute = toAbsoluteIndex(relative);
    auto varSizedData = ref.loadVarSized(absolute, size);

    auto optBuffer = ref.bufferManager->getUnpooledBuffer(size);
    if (!optBuffer) {
        NES_THROW_RUNTIME_ERROR("Out of memory");
    }
    auto* textValue = Nautilus::TextValue::create(optBuffer.value(), size);
    std::ranges::copy(varSizedData, textValue->str());
    return {textValue, static_cast<char*>(offset_ptr) + sizeof(size_t) + sizeof(detail::RelativeVarPageIndex)};
}

std::span<const char> Entry::data() const { return page.data().subspan(offset, entrySize); }

detail::RelativeVarPageIndex Entry::toRelativeIndex(detail::AbsoluteVarPageIndex absolute) const {
    return {static_cast<int32_t>(static_cast<int64_t>(absolute.index) - static_cast<int64_t>(page.varPageBase())),
            absolute.offset};
}

detail::AbsoluteVarPageIndex Entry::toAbsoluteIndex(detail::RelativeVarPageIndex relative) const {
    return {relative.index + page.varPageBase(), relative.offset};
}

detail::AbsoluteVarPageIndex PagedVectorRowLayout::findOrAllocateVarSizePageForSize(size_t size) {
    std::erase_if(varPageOccupation, [](const auto& po) {
        return detail::EntryPage::ENTRY_PAGE_SIZE - po.second < VAR_PAGE_SIZE_THRESHHOLD;
    });

    for (auto& vp : varPageOccupation) {
        if (detail::VarPage::VAR_PAGE_SIZE - vp.second >= size) {
            auto offset = vp.second;
            vp.second += size;
            return {vp.first, offset};
        }
    }
    auto optBuffer = bufferManager->getUnpooledBuffer(detail::VarPage::VAR_PAGE_SIZE);
    if (!optBuffer) {
        NES_THROW_RUNTIME_ERROR("Out of memory");
    }
    varSizedPages.emplace_back(std::move(optBuffer.value()));
    varPageOccupation.emplace_back(varSizedPages.size() - 1, size);
    return {static_cast<uint32_t>(varSizedPages.size() - 1), 0};
}

size_t PagedVectorRowLayout::getEntrySize() const { return entrySize; }

size_t PagedVectorRowLayout::size() const { return numberOfEntries; }
Entry PagedVectorRowLayout::getEntry(EntryRef ref) {
    auto& page = entryPages[ref.entryPageIndex];
    return Entry({}, ref.entryPageIndex, page, *this, ref.entryOffset, entrySize);
}

PagedVectorRowLayout::PagedVectorRowLayout(BufferManagerPtr bufferManager, size_t entrySize, uint64_t entryPageSize)
    : bufferManager(std::move(bufferManager)), entrySize(entrySize) {
    NES_ASSERT(entryPageSize == detail::EntryPage::ENTRY_PAGE_SIZE, "Not properly Implemented. Sorry!");
}
detail::AbsoluteVarPageIndex PagedVectorRowLayout::storeVarSized(std::span<const char> data) {
    auto index = findOrAllocateVarSizePageForSize(data.size());
    auto dest_page_memory = varSizedPages[index.index].data().subspan(index.offset, data.size());
    NES_ASSERT(dest_page_memory.size() == data.size(), "Data does not fit into VarSized Page");
    std::copy(data.begin(), data.end(), dest_page_memory.begin());
    return index;
}

std::span<const char> PagedVectorRowLayout::loadVarSized(detail::AbsoluteVarPageIndex index, size_t size) const {
    return varSizedPages[index.index].data().subspan(index.offset, size);
}

PagedVectorRowLayout::EntryIterator::EntryIterator(PagedVectorRowLayout& ref, size_t index)
    : ref(ref), currentIndex(index), current_occupied(ref.pageOccupation.begin()) {
    if (ref.numberOfEntries == 0) {
        return;
    }

    if (current_occupied == ref.pageOccupation.end() || current_occupied->first != 0) {
        currentPageNumberOfEntries = detail::EntryPage::ENTRY_PAGE_SIZE / ref.entrySize;
    } else {
        currentPageNumberOfEntries = (current_occupied++)->second / ref.entrySize;
    }
}

PagedVectorRowLayout::EntryIterator::reference PagedVectorRowLayout::EntryIterator::operator*() const {
    NES_ASSERT(currentPageOffset < currentPageNumberOfEntries, "PageIndex is Out of bounds");
    NES_ASSERT(currentPageIndex < ref.entryPages.size(), "EntryIndex is Out of bounds");
    NES_ASSERT(currentIndex < ref.numberOfEntries, "Vector Access is Out of bounds");

    auto& page = ref.entryPages[currentPageIndex];

    return Entry({}, currentPageIndex, page, ref, currentPageOffset * ref.entrySize, ref.entrySize);
}

PagedVectorRowLayout::EntryIterator& PagedVectorRowLayout::EntryIterator::operator++() {
    NES_ASSERT(currentIndex < ref.numberOfEntries, "EntryIndex is Out of Bounds");
    currentIndex++;
    if (++currentPageOffset < currentPageNumberOfEntries) {
        return *this;
    }
    if (++currentPageIndex < ref.entryPages.size()) {
        currentPageOffset = 0;
        if (current_occupied == ref.pageOccupation.end() || current_occupied->first != currentPageIndex) {
            currentPageNumberOfEntries = detail::EntryPage::ENTRY_PAGE_SIZE / ref.entrySize;
        } else {
            currentPageNumberOfEntries = (current_occupied++)->second / ref.entrySize;
        }
        return *this;
    }
    NES_ASSERT(currentIndex == ref.numberOfEntries, "EntryIndex is Out of Bounds");
    return *this;
}

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::EntryIterator::operator++(int) {
    EntryIterator tmp = *this;
    ++(*this);
    return tmp;
}

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::begin() { return EntryIterator(*this, 0); }
PagedVectorRowLayout::EntryIterator* PagedVectorRowLayout::beginAlloc() { return new EntryIterator(*this, 0); }

PagedVectorRowLayout::EntryIterator PagedVectorRowLayout::end() { return EntryIterator(*this, numberOfEntries); }
PagedVectorRowLayout::EntryIterator* PagedVectorRowLayout::endAlloc() { return new EntryIterator(*this, numberOfEntries); }

Entry PagedVectorRowLayout::allocateEntry() {
    std::erase_if(pageOccupation, [this](const auto& po) {
        return detail::EntryPage::ENTRY_PAGE_SIZE - po.second < entrySize;
    });

    if (pageOccupation.empty()) {
        auto optBuffer = bufferManager->getUnpooledBuffer(detail::EntryPage::ENTRY_PAGE_SIZE);
        if (!optBuffer) {
            NES_THROW_RUNTIME_ERROR("Out of memory");
        }
        entryPages.emplace_back(std::move(optBuffer.value()));
        entryPages.back().varPageBase() = entryPages.size() - 1;
        pageOccupation.emplace_back(entryPages.size() - 1, 0);
    }

    auto [pageIndex, pageOffset] = pageOccupation.front();
    pageOccupation.front().second += entrySize;
    numberOfEntries++;
    return Entry({}, pageIndex, entryPages[pageIndex], *this, pageOffset, entrySize);
}

void PagedVectorRowLayout::takePagesFrom(PagedVectorRowLayout&& other) {
    NES_ASSERT(entrySize == other.entrySize, "PagedVector EntrySize missmatch");

    auto varPageOffset = varSizedPages.size();
    auto entryPageOffset = entryPages.size();
    entryPages.reserve(entryPages.size() + other.entryPages.size());
    varSizedPages.reserve(varSizedPages.size() + other.varSizedPages.size());

    for (auto& [index, _] : other.pageOccupation) {
        index += entryPageOffset;
    }

    //Update the Base Var Page Index
    for (auto& otherEntryPage : other.entryPages) {
        otherEntryPage.varPageBase() += varPageOffset;
    }

    for (auto& [index, _] : other.varPageOccupation) {
        index += varPageOffset;
    }

    std::ranges::move(other.entryPages, std::back_inserter(entryPages));
    std::ranges::move(other.varSizedPages, std::back_inserter(varSizedPages));
    std::ranges::move(other.pageOccupation, std::back_inserter(pageOccupation));
    std::ranges::move(other.varPageOccupation, std::back_inserter(varPageOccupation));
    numberOfEntries += other.numberOfEntries;
    other.numberOfEntries = 0;
}

bool operator==(const PagedVectorRowLayout::EntryIterator& a, const PagedVectorRowLayout::EntryIterator& b) {
    return a.currentIndex == b.currentIndex;
}

bool operator!=(const PagedVectorRowLayout::EntryIterator& a, const PagedVectorRowLayout::EntryIterator& b) {
    return a.currentIndex != b.currentIndex;
}
}// namespace NES::Runtime
