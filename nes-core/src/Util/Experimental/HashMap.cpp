#include <Util/Experimental/HashMap.hpp>

namespace NES::Experimental {

Hashmap::Entry::Entry(Entry* next, hash_t hash) : next(next), hash(hash) {}

Hashmap::Hashmap(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                 size_t keySize,
                 size_t valueSize,
                 size_t nrEntries)
    : entrySize(headerSize + keySize + valueSize), keyOffset(headerSize), valueOffset(keyOffset + keySize),
      entriesPerBuffer(bufferManager->getBufferSize() / entrySize), bufferManager(bufferManager) {
    setSize(nrEntries);
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

}// namespace NES::Experimental