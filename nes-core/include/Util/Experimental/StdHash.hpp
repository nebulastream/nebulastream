/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
