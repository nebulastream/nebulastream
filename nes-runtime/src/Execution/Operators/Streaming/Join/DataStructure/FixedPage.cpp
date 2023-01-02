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

#include <Execution/Operators/Streaming/Join/DataStructure/FixedPage.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <atomic>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

FixedPage::FixedPage(uint8_t* dataPtr, size_t sizeOfRecord, size_t pageSize)
    : sizeOfRecord(sizeOfRecord), data(dataPtr), capacity(pageSize / sizeOfRecord) {
    NES_ASSERT2_FMT(0 < capacity, "Capacity is zero " << capacity);

    bloomFilter = std::make_unique<BloomFilter>(capacity, BLOOM_FALSE_POSITIVE_RATE);
    currentPos = 0;
}

uint8_t* FixedPage::append(const uint64_t hash) {
    if (currentPos >= capacity) {
        return nullptr;
    }

    bloomFilter->add(hash);
    uint8_t* ptr = &data[currentPos * sizeOfRecord];
    currentPos++;
    return ptr;
}

bool FixedPage::bloomFilterCheck(uint8_t* keyPtr, size_t sizeOfKey) const {
    uint64_t totalKey;
    memcpy(&totalKey, keyPtr, sizeOfKey);
    uint64_t hash = Execution::Util::murmurHash(totalKey);

    return bloomFilter->checkContains(hash);
}

uint8_t* FixedPage::operator[](size_t index) const { return &(data[index * sizeOfRecord]); }

size_t FixedPage::size() const { return currentPos; }

FixedPage::FixedPage(FixedPage&& otherPage)
    : sizeOfRecord(otherPage.sizeOfRecord), data(otherPage.data), currentPos(otherPage.currentPos), capacity(otherPage.capacity),
      bloomFilter(std::move(otherPage.bloomFilter)) {
    otherPage.sizeOfRecord = 0;
    otherPage.data = nullptr;
    otherPage.currentPos = 0;
    otherPage.capacity = 0;
}
FixedPage& FixedPage::operator=(FixedPage&& otherPage) {
    if (this == std::addressof(otherPage)) {
        return *this;
    }

    swap(*this, otherPage);
    return *this;
}

void FixedPage::swap(FixedPage& lhs, FixedPage& rhs) noexcept {
    std::swap(lhs.sizeOfRecord, rhs.sizeOfRecord);
    std::swap(lhs.data, rhs.data);
    std::swap(lhs.currentPos, rhs.currentPos);
    std::swap(lhs.capacity, rhs.capacity);
    std::swap(lhs.bloomFilter, rhs.bloomFilter);
}

FixedPage::FixedPage(FixedPage* otherPage)
    : sizeOfRecord(otherPage->sizeOfRecord), data(otherPage->data), currentPos(otherPage->currentPos),
      capacity(otherPage->capacity), bloomFilter(std::move(otherPage->bloomFilter)) {}
}// namespace NES::Runtime::Execution::Operators
