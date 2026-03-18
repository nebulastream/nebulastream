/*
    Stub replacement for folly/hash/Hash.h.
    Provides folly::hash::hash_combine using std::hash so that
    SourceDescriptor.hpp compiles without the real Folly library.
*/
#pragma once
#include <functional>

namespace folly { namespace hash {
template <typename... Ts>
size_t hash_combine(const Ts&... args) {
    size_t seed = 0;
    (..., (seed ^= std::hash<Ts>{}(args) + 0x9e3779b9 + (seed << 6) + (seed >> 2)));
    return seed;
}
}} // namespace folly::hash
