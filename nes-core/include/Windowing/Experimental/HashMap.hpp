#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Windowing/Experimental/Hash.hpp>
#include <assert.h>
#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_

namespace NES::Experimental {

/**
 * @brief A chained hashmap that is backed by the buffer pool manager.
 */
class Hashmap {
  public:
    const Hash<CRC32Hash> hasher = Hash<CRC32Hash>();
    using hash_t = uint64_t;
    static const size_t headerSize = 16;// next* + hash = 16byte
    const size_t entrySize;
    const size_t keyOffset;
    const size_t valueOffset;
    const size_t entriesPerBuffer;
    class Entry
    /// Header for hashtable entries
    {
      public:
        Entry* next;
        hash_t hash;
        Entry(Entry* next, hash_t hash) : next(next), hash(hash) {}
        // payload data follows this header
    };

    explicit Hashmap(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                     size_t keySize,
                     size_t valueSize,
                     size_t nrEntries)
        : entrySize(headerSize + keySize + valueSize), keyOffset(headerSize), valueOffset(keyOffset + keySize),
          entriesPerBuffer(bufferManager->getBufferSize() / entrySize), bufferManager(bufferManager) {
        setSize(nrEntries);
    }

    /**
     * @brief Get the key for a specific entry
     * @param entry
     * @return
     */
    uint8_t* getKeyPtr(Entry* entry) { return ((uint8_t*) entry) + keyOffset; };

    template<class T>
    T* getKeyPtr(Entry* entry) {
        return (T*) getKeyPtr(entry);
    };

    std::unique_ptr<std::vector<Runtime::TupleBuffer>> getEntries(){
        return std::move(storageBuffers);
    };

    /**
     * @brief Get the value for a specific entry
     * @param entry
     * @return
     */
    uint8_t* getValuePtr(Entry* entry) { return ((uint8_t*) entry) + valueOffset; };

    /**
     * @brief Returns the first entry of the chain for the given hash
     */
    inline Entry* find_chain(hash_t hash);

    /**Returns the first entry of the chain for the given hash
    * Uses pointer tagging as a filter to quickly determine whether hash iscontained
    */
    inline Entry* find_chain_tagged(hash_t hash);
    /// Insert entry into chain for the given hash
    inline void insert(Entry* entry, hash_t hash);
    /// Insert entry into chain for the given hash
    /// Updates tag
    inline void insert_tagged(Entry* entry, hash_t hash);
    /// Insert n entries starting from first, always looking for the next entry
    /// step bytes after the previous
    inline void insertAll(Entry* first, size_t n, size_t step);

    inline void insertAll_tagged(Entry* first, size_t n, size_t step);

    template<typename K, bool useTags>
    inline Entry* findOneEntry(const K& key, hash_t h);
   template<class KeyType>
    inline uint8_t* getEntry(KeyType& key);
    template<typename K, bool useTags>
    Hashmap::Entry* findOrCreate(K key, hash_t hash);
    /// Set size (no resize functionality)
    inline size_t setSize(size_t nrEntries);
    /// Removes all elements from the hashtable
    inline void clear();

    Entry** entries;
    Runtime::TupleBuffer entryBuffer;
    std::unique_ptr<std::vector<Runtime::TupleBuffer>> storageBuffers;

    hash_t mask;
    using ptr_t = uint64_t;
    const ptr_t maskPointer = (~(ptr_t) 0) >> (16);
    const ptr_t maskTag = (~(ptr_t) 0) << (sizeof(ptr_t) * 8 - 16);

    inline static Entry* end();

    Hashmap() = default;

    Hashmap(const Hashmap&) = delete;

    inline ~Hashmap();

    Entry* entryIndexToAddress(uint64_t entry) {
        auto bufferIndex = entry / entriesPerBuffer;
        auto& buffer = (*storageBuffers)[bufferIndex];
        auto entryOffsetInBuffer = entry - (bufferIndex * entriesPerBuffer);
        return reinterpret_cast<Entry*>(buffer.getBuffer() + (entryOffsetInBuffer * entrySize));
    }

    Entry* allocateNewEntry() {
        if (currentSize % entriesPerBuffer == 0) {
            auto buffer = bufferManager->getBufferNoBlocking();
            if (!buffer.has_value()) {
                //     throw Compiler::CompilerException("BufferManager is empty. Size "
                //                                     + std::to_string(bufferManager->getNumOfPooledBuffers()));
            }
            (*storageBuffers).emplace_back(std::move(buffer.value()));
        }
        return entryIndexToAddress(currentSize);
    }

  private:
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    inline Hashmap::Entry* ptr(Hashmap::Entry* p);
    inline ptr_t tag(hash_t p);
    inline Hashmap::Entry* update(Hashmap::Entry* old, Hashmap::Entry* p, hash_t hash);
    size_t capacity = 0;
    uint64_t currentSize = 0;
};

extern Hashmap::Entry notFound;

inline Hashmap::Entry* Hashmap::end() { return nullptr; }

inline Hashmap::~Hashmap() { std::cout << "drop hashtable" << std::endl; }

inline Hashmap::ptr_t Hashmap::tag(Hashmap::hash_t hash) {
    auto tagPos = hash >> (sizeof(hash_t) * 8 - 4);
    return ((size_t) 1) << (tagPos + (sizeof(ptr_t) * 8 - 16));
}

inline Hashmap::Entry* Hashmap::ptr(Hashmap::Entry* p) { return (Entry*) ((ptr_t) p & maskPointer); }

inline Hashmap::Entry* Hashmap::update(Hashmap::Entry* old, Hashmap::Entry* p, Hashmap::hash_t hash) {
    return reinterpret_cast<Entry*>((size_t) p | ((size_t) old & maskTag) | tag(hash));
}

inline Hashmap::Entry* Hashmap::find_chain(hash_t hash) {
    auto pos = hash & mask;
    return entries[pos];
}

void inline Hashmap::insert(Entry* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    auto oldValue = entries[pos];
    entry->next = oldValue;
    entries[pos] = entry;
    this->currentSize++;
}

inline Hashmap::Entry* Hashmap::find_chain_tagged(hash_t hash) {
    //static_assert(sizeof(hash_t) == 8, "Hashtype not supported");
    auto pos = hash & mask;
    auto candidate = entries[pos];
    auto filterMatch = (size_t) candidate & tag(hash);
    if (filterMatch)
        return ptr(candidate);
    else
        return end();
}

void inline Hashmap::insert_tagged(Entry* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    auto oldValue = entries[pos];
    entry->next = oldValue;
    entries[pos] = update(entries[pos], entry, hash);
    this->currentSize++;
}

size_t inline Hashmap::setSize(size_t nrEntries) {
    currentSize = 0;
    assert(nrEntries != 0);
    if (entryBuffer.isValid()) {
        entryBuffer.release();
        entries = nullptr;
        if (storageBuffers != nullptr) {
            storageBuffers->clear();
        }
    }

    const auto loadFactor = 0.7;
    size_t exp = 64 - __builtin_clzll(nrEntries);
    assert(exp < sizeof(hash_t) * 8);
    if (((size_t) 1 << exp) < nrEntries / loadFactor)
        exp++;
    capacity = ((size_t) 1) << exp;
    mask = capacity - 1;
    auto buffer = bufferManager->getUnpooledBuffer(capacity * sizeof(Entry*));
    if (!buffer.has_value()) {
    }
    buffer = buffer.value();
    entries = buffer->getBuffer<Entry*>();
    return capacity * loadFactor;
}

void inline Hashmap::clear() {
    currentSize = 0;
    for (size_t i = 0; i < capacity; i++) {
        entries[i] = nullptr;
    }
}

template<typename K, bool useTags>
inline Hashmap::Entry* Hashmap::findOneEntry(const K& key, hash_t h) {
    Entry* entry;
    if (useTags)
        entry = reinterpret_cast<Entry*>(find_chain_tagged(h));
    else
        entry = reinterpret_cast<Entry*>(find_chain(h));
    if (entry == end())
        return nullptr;
    for (; entry != end(); entry = reinterpret_cast<Entry*>(entry->next))
        if (entry->hash == h && (*getKeyPtr<K>(entry)) == key)
            return entry;
    return nullptr;
}

template<typename K, bool useTags>
inline Hashmap::Entry* Hashmap::findOrCreate(K key, hash_t hash) {
    auto entry = findOneEntry<K, useTags>(key, hash);
    if (!entry) {
        auto newEntry = allocateNewEntry();
        newEntry->hash = hash;
        *getKeyPtr<K>(newEntry) = key;
        insert(newEntry, hash);
        return newEntry;
    }
    return entry;
}
template<class KeyType>
inline uint8_t* getEntry(KeyType& key) {
    const auto h = hasher(key, Experimental::Hash<Experimental::CRC32Hash>::SEED);
    auto entry = findOrCreate<KeyType, false>(key, h);
    return getKeyPtr(entry);
}

class HashMapFactory {
  public:
    HashMapFactory(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                   size_t keySize,
                   size_t valueSize,
                   size_t nrEntries)
        : bufferManager(bufferManager), keySize(keySize), valueSize(valueSize), nrEntries(nrEntries){};
    Hashmap create() { return Hashmap(bufferManager, keySize, valueSize, nrEntries); }

  private:
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    size_t keySize;
    size_t valueSize;
    size_t nrEntries;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_
