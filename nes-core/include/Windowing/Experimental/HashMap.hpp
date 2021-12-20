#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_

namespace NES::Experimental{

class Hashmap {

  public:
    using hash_t = uint64_t;
    size_t capacity = 0;
    class EntryHeader
    /// Header for hashtable entries
    {
      public:
        EntryHeader* next;
        hash_t hash;
        EntryHeader(EntryHeader* n, hash_t h) : next(n), hash(h) {}
        // payload data follows this header
    };
    /// Returns the first entry of the chain for the given hash
    inline EntryHeader* find_chain(hash_t hash);
    /// Returns the first entry of the chain for the given hash
    /// Uses pointer tagging as a filter to quickly determine whether hash is
    /// contained
    inline EntryHeader* find_chain_tagged(hash_t hash);
    /// Insert entry into chain for the given hash
    template <bool concurrentInsert = true>
    inline void insert(EntryHeader* entry, hash_t hash);
    /// Insert entry into chain for the given hash
    /// Updates tag
    template <bool concurrentInsert = true>
    inline void insert_tagged(EntryHeader* entry, hash_t hash);
    /// Insert n entries starting from first, always looking for the next entry
    /// step bytes after the previous
    template <bool concurrentInsert = true>
    inline void insertAll(EntryHeader* first, size_t n, size_t step);
    template <bool concurrentInsert = true>
    inline void insertAll_tagged(EntryHeader* first, size_t n, size_t step);
    /// Set size (no resize functionality)
    inline size_t setSize(size_t nrEntries);
    /// Removes all elements from the hashtable
    inline void clear();

    std::atomic<EntryHeader*>* entries = nullptr;

    hash_t mask;
    using ptr_t = uint64_t;
    const ptr_t maskPointer = (~(ptr_t)0) >> (16);
    const ptr_t maskTag = (~(ptr_t)0) << (sizeof(ptr_t) * 8 - 16);

    inline static EntryHeader* end();
    Hashmap() = default;
    Hashmap(const Hashmap&) = delete;
    inline ~Hashmap();

  private:
    inline Hashmap::EntryHeader* ptr(Hashmap::EntryHeader* p);
    inline ptr_t tag(hash_t p);
    inline Hashmap::EntryHeader* update(Hashmap::EntryHeader* old,
                                        Hashmap::EntryHeader* p, hash_t hash);
};

extern Hashmap::EntryHeader notFound;

inline Hashmap::EntryHeader* Hashmap::end() { return nullptr; }

inline Hashmap::~Hashmap() {

}

inline Hashmap::ptr_t Hashmap::tag(Hashmap::hash_t hash) {
    auto tagPos = hash >> (sizeof(hash_t) * 8 - 4);
    return ((size_t)1) << (tagPos + (sizeof(ptr_t) * 8 - 16));
}

inline Hashmap::EntryHeader* Hashmap::ptr(Hashmap::EntryHeader* p) {
    return (EntryHeader*)((ptr_t)p & maskPointer);
}

inline Hashmap::EntryHeader* Hashmap::update(Hashmap::EntryHeader* old,
                                             Hashmap::EntryHeader* p,
                                             Hashmap::hash_t hash) {
    return reinterpret_cast<EntryHeader*>((size_t)p | ((size_t)old & maskTag) |
                                          tag(hash));
}

inline Hashmap::EntryHeader* Hashmap::find_chain(hash_t hash) {
    auto pos = hash & mask;
    return entries[pos].load(std::memory_order_relaxed);
}

template <bool concurrentInsert>
void inline Hashmap::insert(EntryHeader* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    if (concurrentInsert) {

        auto locPtr = &entries[pos];
        EntryHeader* loc = locPtr->load();
        EntryHeader* newLoc;
        do {
            entry->next = loc;
            newLoc = entry;
        } while (!locPtr->compare_exchange_weak(loc, newLoc));
    } else {
        auto& loc = entries[pos];
        auto oldValue = loc.load(std::memory_order_relaxed);
        entry->next = oldValue;
        loc.store(entry, std::memory_order_relaxed);
    }
}

inline Hashmap::EntryHeader* Hashmap::find_chain_tagged(hash_t hash) {
    //static_assert(sizeof(hash_t) == 8, "Hashtype not supported");
    auto pos = hash & mask;
    auto candidate = entries[pos].load(std::memory_order_relaxed);
    auto filterMatch = (size_t)candidate & tag(hash);
    if (filterMatch)
        return ptr(candidate);
    else
        return end();
}

template <bool concurrentInsert>
void inline Hashmap::insert_tagged(EntryHeader* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    if (concurrentInsert) {
        auto locPtr = &entries[pos];
        EntryHeader* loc = locPtr->load();
        EntryHeader* newLoc;
        do {
            entry->next = ptr(loc);
            newLoc = update(loc, entry, hash);
        } while (!locPtr->compare_exchange_weak(loc, newLoc));
    } else {
        auto& loc = entries[pos];
        auto oldValue = loc.load(std::memory_order_relaxed);
        entry->next = ptr(oldValue);
        loc.store(update(loc, entry, hash), std::memory_order_relaxed);
    }
}

template <bool concurrentInsert>
void inline Hashmap::insertAll(EntryHeader* first, size_t n, size_t step) {
    EntryHeader* e = first;
    for (size_t i = 0; i < n; ++i) {
        insert<concurrentInsert>(e, e->hash);
        e = reinterpret_cast<EntryHeader*>(reinterpret_cast<uint8_t*>(e) + step);
    }
}

template <bool concurrentInsert>
void inline Hashmap::insertAll_tagged(EntryHeader* first, size_t n,
                                      size_t step) {
    EntryHeader* e = first;
    for (size_t i = 0; i < n; ++i) {
        insert_tagged<concurrentInsert>(e, e->hash);
        e = reinterpret_cast<EntryHeader*>(reinterpret_cast<uint8_t*>(e) + step);
    }
}

size_t inline Hashmap::setSize(size_t nrEntries) {
    assert(nrEntries != 0);
    if (entries)
        mem::free_huge(entries, capacity * sizeof(std::atomic<EntryHeader*>));

    const auto loadFactor = 0.7;
    size_t exp = 64 - __builtin_clzll(nrEntries);
    assert(exp < sizeof(hash_t) * 8);
    if (((size_t)1 << exp) < nrEntries / loadFactor) exp++;
    capacity = ((size_t)1) << exp;
    mask = capacity - 1;
    entries = static_cast<std::atomic<EntryHeader*>*>(
        mem::malloc_huge(capacity * sizeof(std::atomic<EntryHeader*>)));
    //clear();
    return capacity * loadFactor;
}

void inline Hashmap::clear() {
    for (size_t i = 0; i < capacity; i++) {
        entries[i].store(end(), std::memory_order_relaxed);
    }
}


}

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASHMAP_HPP_
