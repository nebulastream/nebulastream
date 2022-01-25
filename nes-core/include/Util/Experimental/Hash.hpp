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


#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASH_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASH_HPP_

#include <functional>
#include <tuple>
#include <utility>
#include <x86intrin.h>
#include <cstdint>
namespace NES::Experimental {


template <class F, class... Ts, std::size_t... Is>
void for_each_in_tuple(const std::tuple<Ts...>& tuple, F func,
                       std::index_sequence<Is...>) {
    using expander = int[];
    (void)expander{0, ((void)func(std::get<Is>(tuple)), 0)...};
}

template <class F, class... Ts>
void for_each_in_tuple(const std::tuple<Ts...>& tuple, F func) {
    for_each_in_tuple(tuple, func, std::make_index_sequence<sizeof...(Ts)>());
}

template <class F, class... Ts, std::size_t... Is>
void for_each_in_tuple(const std::tuple<Ts...>&& tuple, F func,
                       std::index_sequence<Is...>) {
    using expander = int[];
    (void)expander{0, ((void)func(std::forward(std::get<Is>(tuple))), 0)...};
}

template <class F, class... Ts>
void for_each_in_tuple(const std::tuple<Ts...>&& tuple, F func) {
    for_each_in_tuple(tuple, func, std::make_index_sequence<sizeof...(Ts)>());
}

template<typename CHILD>
class Hash {

    const CHILD* impl() const { return static_cast<const CHILD*>(this); }

  public:
    static const uint64_t SEED = 902850234;
    using hash_t = std::uint64_t;
    /// Integers
    inline hash_t operator()(uint64_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int64_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(uint32_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int32_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int8_t x, hash_t seed) const { return impl()->hashKey(x, seed); }

    template<typename... T>
    inline hash_t operator()(std::tuple<T...>&& x, hash_t seed) const {
        hash_t hash = seed;
        auto& self = *impl();
        auto f = [&](auto& x) {
            hash = self(x, hash);
        };
        for_each_in_tuple(x, f);
        return hash;
    }
    template<typename... T>
    inline hash_t operator()(const std::tuple<T...>& x, hash_t seed) const {
        hash_t hash = seed;
        auto& self = *impl();
        auto f = [&](const auto& x) {
            hash = self(x, hash);
        };
        for_each_in_tuple(x, f);
        return hash;
    }
};

#define FORCE_INLINE inline __attribute__((always_inline))

}// namespace NES::Experimental

#endif// NES_INCLUDE_RUNTIME_TUPLE_BUFFER_HPP_