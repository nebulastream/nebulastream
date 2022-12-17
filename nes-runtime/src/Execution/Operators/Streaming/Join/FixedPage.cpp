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

#include <atomic>

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/Join/FixedPage.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

FixedPage::FixedPage(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord, size_t pageSize) : sizeOfRecord(sizeOfRecord) {
    auto ptr = tail.fetch_add(pageSize);
    NES_ASSERT2_FMT(ptr < overrunAddress, "Invalid address " << ptr << " < " << overrunAddress);

    capacity = pageSize / sizeOfRecord;
    NES_ASSERT2_FMT(0 < capacity, "Capacity is zero " << capacity);

    data = reinterpret_cast<uint8_t*>(ptr);

    bloomFilter = std::make_unique<BloomFilter>(capacity, BLOOM_FALSE_POSITIVE_RATE);
    pos = 0;
}

uint8_t* FixedPage::append(const uint64_t hash)  {
    if (pos >= capacity) {
        return nullptr;
    }

    bloomFilter->add(hash);
    uint8_t* ptr = &data[pos * sizeOfRecord];
    pos++;
    return ptr;
}


bool FixedPage::bloomFilterCheck(uint8_t* keyPtr, size_t sizeOfKey) const  {
    uint64_t totalKey;
    memcpy(&totalKey, keyPtr, sizeOfKey);
    uint64_t hash = Util::murmurHash(totalKey);

    return bloomFilter->checkContains(hash);
}

uint8_t* FixedPage::operator[](size_t index) const  { return &(data[index * sizeOfRecord]); }

size_t FixedPage::size() const { return pos; }

FixedPage::FixedPage(const FixedPage& that) : sizeOfRecord(that.sizeOfRecord), data(that.data), pos(that.pos),
                                        capacity(that.capacity), bloomFilter(that.bloomFilter.get()){}

FixedPage::FixedPage(FixedPage&& that) : sizeOfRecord(that.sizeOfRecord), data(that.data), pos(that.pos),
                                         capacity(that.capacity), bloomFilter(std::move(that.bloomFilter)) {
    that.sizeOfRecord = 0;
    that.data = nullptr;
    that.pos = 0;
    that.capacity = 0;
}
FixedPage& FixedPage::operator=(FixedPage&& that) {
    if (this == std::addressof(that)) {
        return *this;
    }

    swap(*this, that);
    return *this;
}



void FixedPage::swap(FixedPage& lhs, FixedPage& rhs) noexcept {
    std::swap(lhs.sizeOfRecord, rhs.sizeOfRecord);
    std::swap(lhs.data, rhs.data);
    std::swap(lhs.pos, rhs.pos);
    std::swap(lhs.capacity, rhs.capacity);
    std::swap(lhs.bloomFilter, rhs.bloomFilter);
}

FixedPage::FixedPage(FixedPage* otherPage) : sizeOfRecord(otherPage->sizeOfRecord), data(otherPage->data), pos(otherPage->pos),
                                        capacity(otherPage->capacity), bloomFilter(std::move(otherPage->bloomFilter)) {}


} // namespace NES::Runtime::Execution::Operators
