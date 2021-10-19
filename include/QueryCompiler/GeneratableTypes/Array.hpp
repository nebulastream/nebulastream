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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_ARRAYTYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_ARRAYTYPE_HPP_

#include <algorithm>
#include <array>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace NES::QueryCompilation {
/**
 * @brief        Container for fixed size arrays of primitive types.
 *
 * @tparam T     the type of the elements that belong to the array
 * @tparam size  the fixed-size array's size.
 */
template<typename T, std::size_t s, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class ArrayBase : public std::array<T, s> {
  public:
    /// @brief Public, externally visible type of this array.
    using type = T;

    /// @brief Public, externally visible size of this array.
    static constexpr std::size_t size = s;

    /// @brief Construct from arguments which can be used to construct the underlying std::array `J...`.
    template<typename... J, typename = std::enable_if_t<std::is_constructible_v<std::array<T, size>, std::decay_t<J>...>>>
    inline ArrayBase(J&&... f) noexcept(noexcept(this->runtimeConstructionTest())) : std::array<T, size>(std::forward<J>(f)...) {
        this->runtimeConstructionTest();
    }

    /// @brief Construct from `size` values of type T.
    template<typename... J,
             typename = std::enable_if_t<std::conjunction_v<std::is_same<T, std::decay_t<J>>...> && sizeof...(J) <= size
                                         && (std::is_same_v<T, char> || sizeof...(J) == size)>>
    inline ArrayBase(J... f) noexcept(sizeof...(J) < size || noexcept(this->runtimeConstructionTest()))
        : std::array<T, size>({std::forward<J>(f)...}) {
        if constexpr (sizeof...(J) == size) {
            this->runtimeConstructionTest();
        }
    }

    /// @brief Construct from c-style array `val`.
    constexpr ArrayBase(T const (&val)[size]) noexcept(noexcept(this->runtimeConstructionTest())) : std::array<T, size>() {
        this->initializeFrom(val, size, std::make_integer_sequence<std::size_t, size>());
        this->runtimeConstructionTest();
    }

    /***
     * @brief Construct from std::vector `vec`.
     *
     * @throws std::runtime_error if the vector's size does not match `size`.
     *
     */
    inline ArrayBase(std::vector<T> const& vec) noexcept(false) : std::array<T, size>() {
        this->initializeFrom(vec, vec.size(), std::make_integer_sequence<std::size_t, size>());
        runtimeConstructionTest();
    }

  protected:
    /***
     * @brief Protected constructor which is usable by partial specializations of the base class.
     * @tparam E
     * @param v
     * @param pSize
     * @throws std::runtime_error if the reported size `pSize` does not match `size`.
     */
    template<bool conductConstructionTest, typename E>
    constexpr ArrayBase(E const* v, std::size_t pSize, std::integral_constant<bool, conductConstructionTest>) noexcept(false)
        : std::array<T, size>() {
        this->initializeFrom(v, pSize, std::make_integer_sequence<std::size_t, size>());
        if constexpr (conductConstructionTest) {
            runtimeConstructionTest();
        }
    }

  private:
    /**
     * @brief Initialization helper, which produces a constexpr loop
     *
     * @tparam E   Type of the content which we assign to the std::array base class.
     * @tparam is  sequence `0...size` used to initialize the content of the underlying array from `payload`.
     *
     * @param payload   Content which we assign to the std::array base class.
     * @param pSize     The size of `payload`.
     *
     * @throws std::runtime_error if the reported size `pSize` does not match `size`.
     */
    template<typename E, std::size_t... is>
    constexpr void
    initializeFrom(E const& payload, std::size_t pSize, std::integer_sequence<std::size_t, is...>) noexcept(false) {

        // Chars are allowed to be of smaller size than
        if constexpr (std::is_same_v<T, char>) {
            if (pSize > size) {
                throw std::runtime_error("The initializer passed to NES::ArrayType is too long for the contained type.");
            }
            std::memcpy(&(this[0]), &(payload[0]), pSize);
        } else {
            if (pSize != size) {
                throw std::runtime_error("The initializer's size does not match the size of the NES::ArrayType that "
                                         "is to be constructed.");
            }
            (this->init<is>(payload), ...);
        }
    }

    /**
     * @brief Helper function which initializes a single element from underlying std::array.
     *
     * @tparam i  array index with 0 <= i < size.
     * @tparam E  Type of the content which we assign to the std::array base class.
     */
    template<std::size_t i, typename E, typename = std::enable_if_t<i<size>> constexpr void init(E const& v) noexcept {
        (*this)[i] = v[i];
    }

    /// @brief Conduct Runtime tests that ensure that the datatype is constructed.
    inline void runtimeConstructionTest() const noexcept(!std::is_same_v<std::decay_t<T>, char>) {
        if constexpr (std::is_same_v<std::decay_t<T>, char>) {
            if (!std::any_of(this->rbegin(), this->rend(), [](char f) {
                    return f == '\0';
                })) {
                throw std::runtime_error("Fixed Size character arrays have to contain a null-terminator. "
                                         "Increase the string's size and add a null terminator to the user-defined "
                                         "string.");
            }
        }
    }
};

/**
 * @brief        Default ArrayType template which defaults to ArrayBase.
 *
 * @tparam T     the type of the elements that belong to the array
 * @tparam size  the fixed-size array's size.
 *
 * @see ArrayBase<T, size>
 */
template<typename T, std::size_t size, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class Array final : public ArrayBase<T, size> {
  public:
    using ArrayBase<T, size>::ArrayBase;
};

/**
 * @brief        Partial specialization which adds additional functionality for arrays of type char.
 *               Unlike fixed-size arrays of type != char, fixed size char arrays are allowed to contain less characters
 *               than specified by size.
 *
 * @tparam size  the fixed-size array's size.
 *
 * @see ArrayBase<char, size>
 */
template<std::size_t size>
class Array<char, size> final : public ArrayBase<char, size> {

  public:
    using ArrayBase<char, size>::ArrayBase;// XXX: Array base with zero args

    /**
     * @brief Create array from std::string, storing the null-terminator at the string's end.
     *        `size` <= str.size()).
     *
     * @param str string from which the NES::Array is constructed.
     *
     * @throws std::runtime_error if the string's size is too large to fit into `size`.
     */
    constexpr Array(std::string&& str) noexcept(false) : ArrayBase<char, size>(str.c_str(), str.size() + 1, std::false_type{}) {}

    inline friend std::ostream& operator<<(std::ostream& os, const Array<char, size>& array) {
        for (size_t i = 0; i < size; i++) {
            os << array[i];
        }
        return os;
    };
};

// Default the operators on ArrayBase to the existing operators on std::array.

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator==(Array<T, sl> const&& l, Array<T, sl> const&& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) == 0;
    } else {
        return static_cast<std::array<T, sl>>(l) == static_cast<std::array<T, sr>>(r);
    }
}

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator>=(Array<T, sl> const& l, Array<T, sl> const& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) >= 0;
    } else {
        return static_cast<std::array<T, sl>>(l) >= static_cast<std::array<T, sr>>(r);
    }
}

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator>(Array<T, sl> const& l, Array<T, sl> const& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) > 0;
    } else {
        return static_cast<std::array<T, sl>>(l) > static_cast<std::array<T, sr>>(r);
    }
}

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator<=(Array<T, sl> const& l, Array<T, sl> const& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) <= 0;
    } else {
        return static_cast<std::array<T, sl>>(l) <= static_cast<std::array<T, sr>>(r);
    }
}

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator<(Array<T, sl> const& l, Array<T, sl> const& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) < 0;
    } else {
        return static_cast<std::array<T, sl>>(l) < static_cast<std::array<T, sr>>(r);
    }
}

template<typename T,
         std::size_t sl,
         std::size_t sr,
         typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, char> || sl == sr>>
inline bool operator!=(Array<T, sl> const& l, Array<T, sl> const& r) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, char> && std::is_same_v<std::decay_t<T>, char>) {
        return std::strcmp(&(l[0]), &(r[0])) != 0;
    } else {
        return static_cast<std::array<T, sl>>(l) != static_cast<std::array<T, sr>>(r);
    }
}

// Boilerplate operators which allowing arguments on LHS and RHS which constructs a std::array of the type that is
// determined by the other operand.

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator==(Array<T, s> const& l, O&& os) noexcept {
    return l == Array<T, s>{std::forward<O>(os)};
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator==(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} == l;
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator>=(Array<T, s> const& l, O&& os) noexcept {
    return l >= Array<T, s>{std::forward<O>(os)};
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator>=(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} >= l;
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator>(Array<T, s> const& l, O&& os) noexcept {
    return l > Array<T, s>{std::forward<O>(os)};
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator>(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} > l;
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator<=(Array<T, s> const& l, O&& os) noexcept {
    return l <= Array<T, s>{std::forward<O>(os)};
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator<=(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} <= l;
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator<(Array<T, s> const& l, O&& os) noexcept {
    return l < Array<T, s>{std::forward<O>(os)};
}
template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator<(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} < l;
}

template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator!=(Array<T, s> const& l, O&& os) noexcept {
    return l != Array<T, s>{std::forward<O>(os)};
}
template<typename T,
         std::size_t s,
         typename O,
         typename = std::enable_if_t<std::is_constructible_v<Array<T, s>, O> && !std::is_same_v<Array<T, s>, std::decay_t<O>>>>
inline bool operator!=(O&& os, Array<T, s> const& l) noexcept {
    return Array<T, s>{std::forward<O>(os)} != l;
}

/// Compiler hint which computes the size of a NES::Array and its type from the arguments.
template<typename T,
         typename... Ts,
         typename = std::enable_if_t<std::conjunction_v<std::is_same<std::decay_t<T>, std::decay_t<Ts>>...>>>
Array(T, Ts...) -> Array<T, sizeof...(Ts) + 1>;

/// std::array
template<typename J, std::size_t size>
Array(std::array<J, size>&&) -> Array<J, size>;

/// c-style array
template<typename J>
Array(J const& array) -> Array<std::decay_t<decltype(array[0])>, std::extent<J>::value>;
}// namespace NES::QueryCompilation

/**
 * Specialize the std::hash for NES Array data type
 * @tparam T data type stored in the array
 * @tparam N size of the array
 */
template<class T, size_t N>
struct std::hash<NES::QueryCompilation::Array<T, N>> {
    auto operator()(const NES::QueryCompilation::Array<T, N>& key) const {
        std::hash<T> hasher;
        size_t result = 0;
        for (size_t i = 0; i < N; ++i) {// FIXME: Potential bottleneck (#2014)
            // taken from https://codereview.stackexchange.com/questions/171999/specializing-stdhash-for-stdarray
            result = result * 31 + hasher(key[i]);
        }
        return result;
    }
};

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_ARRAYTYPE_HPP_
