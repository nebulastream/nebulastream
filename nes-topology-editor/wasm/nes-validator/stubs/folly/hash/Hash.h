#pragma once
#include <cstddef>
#include <functional>

namespace folly::hash {

struct StdHasher {
    template<typename T>
    size_t operator()(const T& val) const { return std::hash<T>{}(val); }
};

template<typename... Args>
size_t hash_combine(Args... args) {
    size_t seed = 0;
    ((seed ^= std::hash<Args>{}(args) + 0x9e3779b9 + (seed << 6) + (seed >> 2)), ...);
    return seed;
}

template<typename Seed, typename Hasher, typename Iter>
size_t commutative_hash_combine_range_generic(Seed seed, Hasher hasher, Iter begin, Iter end) {
    size_t result = seed;
    for (auto it = begin; it != end; ++it) {
        result ^= hasher(*it) + 0x9e3779b9 + (result << 6) + (result >> 2);
    }
    return result;
}

}
