//
// Created by Anastasiia Kozar on 01.10.21.
//

#include <Runtime/BufferStorage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <queue>

namespace NES {
    void BufferStorage::insertBuffer(uint64_t id, NES::Runtime::TupleBuffer bufferPtr) {
        buffer.push(std::pair<uint64_t, Runtime::TupleBuffer> {id, bufferPtr});
    }

    bool BufferStorage::trimBuffer(uint64_t id) {
        if(!buffer.empty()) {
            while (buffer.front().first < id) {
                buffer.pop();
                return true;
            }
        }
        return false;
    }

    size_t  BufferStorage::getStorageSize() {
        return buffer.size();
    }

}// namespace NES