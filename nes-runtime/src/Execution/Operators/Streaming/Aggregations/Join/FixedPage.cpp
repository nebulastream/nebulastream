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

#include <Nautilus/Interface/Record.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/FixedPage.hpp>

namespace NES::Runtime::Execution::Operators {

FixedPage::FixedPage(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord) {
    auto ptr = tail.fetch_add(CHUNK_SIZE);
    NES_ASSERT2_FMT(ptr < overrunAddress, "Invalid address " << ptr << " < " << overrunAddress);
    data = reinterpret_cast<Nautilus::Record*>(ptr);

    capacity = CHUNK_SIZE / sizeOfRecord;
    bloomFilter = std::make_unique<BloomFilter>(capacity, BLOOM_FALSE_POSITIVE_RATE);
}


bool FixedPage::append(const uint64_t hash, const Nautilus::Record& record)  {
    data[pos++] = record;
    bloomFilter->add(hash);
    return pos < capacity;

    next step is to integrate the localhashtable into the build of one stream phase
    afterwards, think how if a second operator might be necessary that does the actual join (so comparing both buckets)
}

bool FixedPage::bloomFilterCheck(uint64_t key) const  { return bloomFilter->checkContains(key); }

Nautilus::Record& FixedPage::operator[](size_t index) const  { return data[index]; }

} // namespace NES::Runtime::Execution::Operators
