#include <Nautilus/Tracing/NativeTagRecorder.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>

namespace NES::Nautilus::Tracing {
static constexpr uint64_t MAX_TAG_SIZE = 20;

void* returnAddress(int offset)
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
    }
    return 0;
}

TagAddress NativeTagRecorder::createStartAddress() { return (uint64_t) (returnAddress(3)); }

Tag NativeTagRecorder::createTag(TagAddress startAddress) {
    std::vector<TagAddress> addresses(MAX_TAG_SIZE);
    // truncate tags to startAddress
    for (uint64_t i = 0; i < MAX_TAG_SIZE; i++) {
        auto address = returnAddress(i);
        if (address != nullptr) {
            addresses.push_back((uint64_t) address);
            if ((uint64_t) address == startAddress) {
                break;
            }
        }
    }
    return {addresses};
}
};// namespace NES::Nautilus::Tracing