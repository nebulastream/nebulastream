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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Util/Logger/Logger.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <atomic>

namespace NES::Nautilus::Interface {

FixedPage::FixedPage(int8_t* dataPtr, size_t sizeOfRecord, size_t pageSize, double bloomFalsePosRate)
    : sizeOfRecord(sizeOfRecord), data(dataPtr), capacity(pageSize / sizeOfRecord), bloomFalsePosRate(bloomFalsePosRate) {
    NES_ASSERT2_FMT(0 < capacity, "Capacity is zero for pageSize " + std::to_string(pageSize) + " and sizeOfRecord "
                        + std::to_string(sizeOfRecord));

    bloomFilter = std::make_unique<Runtime::BloomFilter>(capacity, bloomFalsePosRate);
    currentPos = 0;
}

int8_t* FixedPage::append(const uint64_t hash) {
    if (currentPos >= capacity) {
        return nullptr;
    }
    addHashToBloomFilter(hash);

    auto ptr = &data[currentPos * sizeOfRecord];
    currentPos++;
    return ptr;
}

void FixedPage::addHashToBloomFilter(const uint64_t hash) {
    if (bloomFilter == nullptr) {
        NES_ERROR("Bloomfilter become empty")
    }
    bloomFilter->add(hash);
}

std::string FixedPage::getContentAsString(SchemaPtr schema) const {
    std::stringstream ss;
    //for each item in the page
    for (auto i = 0UL; i < currentPos; i++) {
        ss << "Page entry no=" << i;
        for (auto u = 0UL; u < schema->getSize(); u++) {
            ss << " field=" << schema->get(u)->getName();
            NES_ASSERT(schema->get(u)->getDataType()->isNumeric(), "This method is only supported for uint64");
            int8_t* pointer = &data[i * sizeOfRecord];
            auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
            for (auto& field : schema->fields) {
                if (field->getName() == schema->get(u)->getName()) {
                    break;
                }
                auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
                pointer += fieldType->size();
            }
            ss << " value=" << (uint64_t) *pointer << std::endl;
        }
    }
    return ss.str();
}

bool FixedPage::bloomFilterCheck(const uint64_t hash) const {
    return bloomFilter->checkContains(hash);
}

int8_t* FixedPage::operator[](size_t index) const { return &(data[index * sizeOfRecord]); }

size_t FixedPage::size() const { return currentPos; }

FixedPage::FixedPage(FixedPage&& otherPage)
    : sizeOfRecord(otherPage.sizeOfRecord), data(otherPage.data), currentPos(otherPage.currentPos.load()),
      capacity(otherPage.capacity), bloomFilter(std::move(otherPage.bloomFilter)) {
    otherPage.sizeOfRecord = 0;
    otherPage.data = nullptr;
    otherPage.currentPos = 0;
    otherPage.capacity = 0;
    otherPage.bloomFilter = std::make_unique<Runtime::BloomFilter>(capacity, bloomFalsePosRate);
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
    lhs.currentPos.store(rhs.currentPos.load());
    std::swap(lhs.capacity, rhs.capacity);
    std::swap(lhs.bloomFilter, rhs.bloomFilter);
}

FixedPage::FixedPage(FixedPage* otherPage)
    : sizeOfRecord(otherPage->sizeOfRecord), data(otherPage->data), currentPos(otherPage->currentPos.load()),
      capacity(otherPage->capacity), bloomFilter(std::move(otherPage->bloomFilter)) {
    otherPage->bloomFilter = std::make_unique<Runtime::BloomFilter>(capacity, bloomFalsePosRate);
}
}// namespace NES::Nautilus::Interface
