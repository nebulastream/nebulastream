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
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <Runtime/BloomFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>

namespace NES::Runtime {

BloomFilter::BloomFilter(uint64_t entries, double falsePositiveRate) {
    NES_ASSERT2_FMT(falsePositiveRate < 1.0 && falsePositiveRate > 0.0,
                    "BloomFilter false positive rate has to be in the range of (0.0, 1.0)!");

    double numerator = std::log(falsePositiveRate);
    double denominator = std::pow(std::log(2), 2);
    double bitsPerEntry = -(numerator / denominator);

    noBits = ((double) entries) * bitsPerEntry;

    uint64_t bytes = noBits / 8;
    if (noBits % 8) {
        bytes += 1;
    }

    noHashes = (uint16_t) std::ceil(std::log(2) * bitsPerEntry);
    bitField = NES::Runtime::detail::allocAligned<uint8_t>(bytes, 64);
    std::memset(bitField, 0, bytes);
}

void BloomFilter::add(uint64_t hash) {
    uint32_t lower32Bits = hash & ((1UL << 32) - 1);
    uint32_t upper32Bits = hash >> 32;

    for (uint16_t i = 0; i < noHashes; ++i) {
        uint32_t x = (lower32Bits + i * upper32Bits) % noBits;
        *(bitField + (x >> 3)) |= (1 << (x & 7));
    }
}

bool BloomFilter::checkContains(uint64_t hash) {
    uint16_t hits = 0;

    uint32_t lower32Bits = hash & ((1UL << 32) - 1);
    uint32_t upper32Bits = hash >> 32;

    for (uint16_t i = 0; i < noHashes; ++i) {
        uint32_t x = (lower32Bits + i * upper32Bits) % noBits;
        if (*(bitField + (x >> 3)) & (1 << (x & 7))) {
            ++hits;
        } else {
            return false;
        }
    }

    return (hits == noHashes);
}

BloomFilter::~BloomFilter() {
    if (bitField) {
        delete[] bitField;
    }
}

}// namespace NES::Runtime
