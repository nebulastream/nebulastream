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
#include <Nautilus/DataStructures/SerializablePagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <type_traits>

namespace NES::DataStructures {

SerializablePagedVector::SerializablePagedVector(PagedVectorState& state) : state(state) {}

void SerializablePagedVector::appendPageIfFull(AbstractBufferProvider* /*bufferProvider*/, const MemoryLayout* memoryLayout)
{
    if (memoryLayout) {
        state.pageSize = memoryLayout->getBufferSize();
        state.entrySize = memoryLayout->getTupleSize();
    }
    if (state.pageSize == 0 || state.entrySize == 0) return;

    if (state.pages.empty()) {
        PagedVectorState::PageData page{};
        page.buffer.resize(state.pageSize);
        page.bufferSize = state.pageSize;
        page.numberOfEntries = 0;
        page.cumulativeSum = 0;
        state.pages.emplace_back(std::move(page));
        cachedBuffers.resize(1);
        createTupleBuffer(0);
        return;
    }

    const size_t last = state.pages.size() - 1;
    createTupleBuffer(last);
    auto& tb = *cachedBuffers[last];
    const uint64_t tuples = tb.getNumberOfTuples();
    const uint64_t capacity = state.pageSize / state.entrySize;
    if (tuples >= capacity) {
        PagedVectorState::PageData page{};
        page.buffer.resize(state.pageSize);
        page.bufferSize = state.pageSize;
        page.numberOfEntries = 0;
        page.cumulativeSum = state.totalEntries;
        state.pages.emplace_back(std::move(page));
        cachedBuffers.resize(state.pages.size());
        createTupleBuffer(state.pages.size() - 1);
    }
}

void SerializablePagedVector::moveAllPages(SerializablePagedVector& other)
{
    for (auto& p : other.state.pages) {
        state.pages.emplace_back(std::move(p));
    }
    other.state.pages.clear();
    cachedBuffers.clear();
    cachedBuffers.resize(state.pages.size());
    updateCumulativeSums();
}

void SerializablePagedVector::copyFrom(const SerializablePagedVector& other)
{
    state = other.state;
    cachedBuffers.clear();
    cachedBuffers.resize(state.pages.size());
}

const TupleBuffer* SerializablePagedVector::getTupleBufferForEntry(uint64_t entryPos) const
{
    if (state.pages.empty()) return nullptr;
    const_cast<SerializablePagedVector*>(this)->updateCumulativeSums();
    const size_t idx = findPageIndex(entryPos);
    createTupleBuffer(idx);
    return cachedBuffers[idx].get();
}

std::optional<uint64_t> SerializablePagedVector::getBufferPosForEntry(uint64_t entryPos) const
{
    if (state.pages.empty()) return std::nullopt;
    const_cast<SerializablePagedVector*>(this)->updateCumulativeSums();
    const size_t idx = findPageIndex(entryPos);
    const auto& page = state.pages[idx];
    return entryPos - page.cumulativeSum;
}

uint64_t SerializablePagedVector::getTotalNumberOfEntries() const
{
    const_cast<SerializablePagedVector*>(this)->updateCumulativeSums();
    return state.totalEntries;
}

const TupleBuffer& SerializablePagedVector::getLastPage() const
{
    const size_t idx = state.pages.empty() ? 0 : state.pages.size() - 1;
    createTupleBuffer(idx);
    return *cachedBuffers[idx];
}

const TupleBuffer& SerializablePagedVector::getFirstPage() const
{
    const size_t idx = 0;
    createTupleBuffer(idx);
    return *cachedBuffers[idx];
}

const PagedVectorState::PageData& SerializablePagedVector::getPageData(size_t index) const { return state.pages[index]; }

size_t SerializablePagedVector::findPageIndex(uint64_t entryPos) const
{
    for (size_t i = 0; i < state.pages.size(); ++i) {
        const auto& p = state.pages[i];
        const uint64_t end = p.cumulativeSum + p.numberOfEntries;
        if (entryPos < end) return i;
    }
    return state.pages.empty() ? 0 : state.pages.size() - 1;
}

void SerializablePagedVector::setMemoryLayout(const MemoryLayout* layout)
{
    if (layout) {
        state.pageSize = layout->getBufferSize();
        state.entrySize = layout->getTupleSize();
    }
    serializeMemoryLayout(layout);
}

std::unique_ptr<MemoryLayout> SerializablePagedVector::getMemoryLayout() const
{
    // Not required for our current join paths; return nullptr to avoid abstract construction
    return nullptr;
}

void SerializablePagedVector::updateCumulativeSums()
{
    uint64_t sum = 0;
    for (size_t i = 0; i < state.pages.size(); ++i) {
        createTupleBuffer(i);
        const TupleBuffer& tb = *cachedBuffers[i];
        const uint64_t tuples = tb.getNumberOfTuples();
        state.pages[i].numberOfEntries = tuples;
        state.pages[i].cumulativeSum = sum;
        sum += tuples;
    }
    state.totalEntries = sum;
}

void SerializablePagedVector::createTupleBuffer(size_t pageIndex) const
{
    if (pageIndex >= cachedBuffers.size()) cachedBuffers.resize(pageIndex + 1);
    if (cachedBuffers[pageIndex]) return;
    auto& page = state.pages[pageIndex];
    // Wrap page.buffer memory into a TupleBuffer view
    auto* nonConst = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(page.buffer.data()));
    TupleBuffer tb = TupleBuffer::reinterpretAsTupleBuffer(nonConst);
    cachedBuffers[pageIndex] = std::make_unique<TupleBuffer>(std::move(tb));
}

void SerializablePagedVector::serializeMemoryLayout(const MemoryLayout* /*layout*/)
{
    state.hasMemoryLayout = false;
}

}
