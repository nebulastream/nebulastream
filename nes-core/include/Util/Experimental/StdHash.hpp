#ifndef NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_STDHASH_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_STDHASH_HPP_
#include <Util/Experimental/Hash.hpp>
namespace NES::Experimental {
class StdHash {
  public:
    using hash_t = size_t;
    /// Integers
    inline hash_t operator()(uint64_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(int64_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(uint32_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(int32_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }

    /// Pointers
    inline hash_t operator()(void* ptr, hash_t seed) const { return std::hash<void*>()(ptr) ^ (seed << 7) ^ (seed >> 2); }

    template<typename X>
    inline hash_t operator()(const X& x, hash_t seed) const {
        return std::hash<X>()(x) ^ (seed << 7) ^ (seed >> 2);
    }
};
}// namespace NES::Experimental

#endif//NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_STDHASH_HPP_
