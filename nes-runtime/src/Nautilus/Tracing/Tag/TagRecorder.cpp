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

#include <Nautilus/Exceptions/TagCreationException.hpp>
#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <execinfo.h>

namespace NES::Nautilus::Tracing {
TagRecorder::TagRecorder(TagAddress startAddress) : startAddress(startAddress) {}

TagVector TagRecorder::createBaseTag() {
    void* tagBuffer[MAX_TAG_SIZE];
    int size = backtrace(tagBuffer, MAX_TAG_SIZE);
    std::vector<TagAddress> addresses;
    for (int i = 0; i < size; i++) {
        addresses.emplace_back((TagAddress) tagBuffer[i]);
    }
    return {addresses};
}

TagAddress TagRecorder::getBaseAddress(TagVector& tag1, TagVector& tag2) {
    auto& tag1Addresses = tag1.getAddresses();
    auto& tag2Addresses = tag2.getAddresses();
    auto size = std::min(tag1Addresses.size(), tag2Addresses.size());
    size_t index = 0;
    while (index < size && tag1Addresses[index] == tag2Addresses[index]) {
        index++;
    }
    while (index < size && tag1Addresses[index] != tag2Addresses[index]) {
        index++;
    }
    if (index >= size) {
        return -1;
    }
    return tag1Addresses[index];
}

void* getReturnAddress(uint32_t offset);

// backtrace it not defined on musl.
#ifndef HOST_IS_MUSL
Tag* TagRecorder::createReferenceTag(TagAddress startAddress) {
    auto* currentTagNode = &rootTagThreeNode;
    for (size_t i = 0; i <= MAX_TAG_SIZE; i++) {
        auto tagAddress = (TagAddress) getReturnAddress(i);
        if (tagAddress == startAddress) {
            return currentTagNode;
        }
        currentTagNode = currentTagNode->append(tagAddress);
    }
    throw TagCreationException("Stack is too deep. This could indicate the use of recursive control-flow,"
                               " which is not supported in Nautilus code.");
}
#else
Tag Tag::createTag(uint64_t) { NES_NOT_IMPLEMENTED(); }
#endif

void* getReturnAddress(uint32_t offset)
#pragma GCC diagnostic ignored "-Wframe-address"
{
    switch (offset) {
        case 0: return __builtin_return_address(0 + 1);
        case 1: return __builtin_return_address(1 + 1);
        case 2: return __builtin_return_address(2 + 1);
        case 3: return __builtin_return_address(3 + 1);
        case 4: return __builtin_return_address(4 + 1);
        case 5: return __builtin_return_address(5 + 1);
        case 6: return __builtin_return_address(6 + 1);
        case 7: return __builtin_return_address(7 + 1);
        case 8: return __builtin_return_address(8 + 1);
        case 9: return __builtin_return_address(9 + 1);
        case 10: return __builtin_return_address(10 + 1);
        case 11: return __builtin_return_address(11 + 1);
        case 12: return __builtin_return_address(12 + 1);
        case 13: return __builtin_return_address(13 + 1);
        case 14: return __builtin_return_address(14 + 1);
        case 15: return __builtin_return_address(15 + 1);
        case 16: return __builtin_return_address(16 + 1);
        case 17: return __builtin_return_address(17 + 1);
        case 18: return __builtin_return_address(18 + 1);
        case 19: return __builtin_return_address(19 + 1);
        case 20: return __builtin_return_address(20 + 1);
        case 21: return __builtin_return_address(21 + 1);
        case 22: return __builtin_return_address(22 + 1);
        case 23: return __builtin_return_address(23 + 1);
        case 24: return __builtin_return_address(24 + 1);
        case 25: return __builtin_return_address(25 + 1);
        case 26: return __builtin_return_address(26 + 1);
        case 27: return __builtin_return_address(27 + 1);
        case 28: return __builtin_return_address(28 + 1);
        case 29: return __builtin_return_address(29 + 1);
        case 30: return __builtin_return_address(30 + 1);
        default: return nullptr;
    }
}

}// namespace NES::Nautilus::Tracing