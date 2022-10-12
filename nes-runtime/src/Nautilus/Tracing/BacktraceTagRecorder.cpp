#include <Nautilus/Tracing/BacktraceTagRecorder.hpp>
#include <execinfo.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing {
static constexpr uint64_t MAX_TAG_SIZE = 20;

TagAddress BacktraceTagRecorder::createStartAddress() {
    void* tagBuffer[MAX_TAG_SIZE];
    auto bt_size = backtrace(tagBuffer, MAX_TAG_SIZE);
    auto bt_syms = backtrace_symbols(tagBuffer, bt_size);
    for (auto i = 1; i < bt_size; i++) {
        NES_DEBUG(bt_syms[i]);
    }
    return reinterpret_cast<TagAddress>(tagBuffer[3]);
}

Tag BacktraceTagRecorder::createTag(TagAddress startAddress) {
    // In the following we use backtrace from glibc to extract the return address pointers.
    void* tagBuffer[MAX_TAG_SIZE];
    int size = backtrace(tagBuffer, MAX_TAG_SIZE);
    std::vector<TagAddress> addresses;
    // truncate tags to startAddress
    for (int i = 0; i < size; i++) {
        auto address = (TagAddress) tagBuffer[i];
        if (address == startAddress) {
            size = i;
            break;
        }
    }

    for (int i = 0; i < size - 1; i++) {
        auto address = (TagAddress) tagBuffer[i];
        addresses.push_back(address);
    }
    return {addresses};
}
};// namespace NES::Nautilus::Tracing