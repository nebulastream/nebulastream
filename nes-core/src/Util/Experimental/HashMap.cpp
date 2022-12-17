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
#include <Util/Experimental/HashMap.hpp>

namespace NES::Experimental {

Hashmap::Entry::Entry(Entry* next, hash_t hash) : next(next), hash(hash) {}

Hashmap::Hashmap(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                 size_t keySize,
                 size_t valueSize,
                 uint64_t nrEntries)
    : entrySize(headerSize + keySize + valueSize), keyOffset(headerSize), valueOffset(keyOffset + keySize),
      entriesPerBuffer(bufferManager->getBufferSize() / entrySize), bufferManager(bufferManager), keySize(keySize) {
    setSize(nrEntries);
}

uint64_t Hashmap::setSize(uint64_t nrEntries) {
    NES_ASSERT(nrEntries != 0, "invalid num entries");

    currentSize = 0;
    if (!!entryBuffer) {
        entryBuffer.release();
        entries = nullptr;
        if (storageBuffers != nullptr) {
            storageBuffers->clear();
        }
    }

    const auto loadFactor = 0.7;
    size_t exp = 64 - __builtin_clzll(nrEntries);
    NES_ASSERT(exp < sizeof(hash_t) * 8, "invalid exp");
    if (((size_t) 1 << exp) < nrEntries / loadFactor) {
        exp++;
    }
    capacity = ((size_t) 1) << exp;
    mask = capacity - 1;
    auto buffer = bufferManager->getUnpooledBuffer(capacity * sizeof(Entry*));
    if (!buffer.has_value()) {
        NES_FATAL_ERROR("No buffer available");
    }
    this->entryBuffer = buffer.value();
    buffer.reset();
    // set entries to zero
    memset(entryBuffer.getBuffer(), 0, entryBuffer.getBufferSize());
    entries = entryBuffer.getBuffer<Entry*>();

    if (storageBuffers == nullptr) {
        storageBuffers = std::make_unique<std::vector<Runtime::TupleBuffer>>();
    }

    return capacity * loadFactor;
}

std::unique_ptr<std::vector<Runtime::TupleBuffer>> Hashmap::extractEntries() { return std::move(storageBuffers); };
std::unique_ptr<std::vector<Runtime::TupleBuffer>>& Hashmap::getEntries() { return storageBuffers; };
Hashmap::Entry* Hashmap::allocateNewEntry() {
    if (currentSize % entriesPerBuffer == 0) {
        auto buffer = bufferManager->getBufferNoBlocking();
        if (!buffer.has_value()) {
            //     throw Compiler::CompilerException("BufferManager is empty. Size "
            //                                     + std::to_string(bufferManager->getNumOfPooledBuffers()));
        }
        // set entries to zero
        memset(buffer->getBuffer(), 0, buffer->getBufferSize());
        (*storageBuffers).emplace_back(buffer.value());
    }
    auto buffer = getBufferForEntry(currentSize);
    buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    return entryIndexToAddress(currentSize);
}

Hashmap::~Hashmap() {
    if (storageBuffers) {
        storageBuffers->clear();
    }
}

HashMapFactory::HashMapFactory(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                               size_t keySize,
                               size_t valueSize,
                               size_t nrEntries)
    : bufferManager(bufferManager), keySize(keySize), valueSize(valueSize), nrEntries(nrEntries) {}

Hashmap HashMapFactory::create() { return Hashmap(bufferManager, keySize, valueSize, nrEntries); }
std::unique_ptr<Hashmap> HashMapFactory::createPtr() {
    return std::make_unique<Hashmap>(bufferManager, keySize, valueSize, nrEntries);
}

size_t HashMapFactory::getKeySize() { return keySize; }

size_t HashMapFactory::getValueSize() { return valueSize; }

}// namespace NES::Experimental