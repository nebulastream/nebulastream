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

#ifndef NES_INCLUDE_COMMON_EXECUTABLETYPE_TENSOR_HPP
#define NES_INCLUDE_COMMON_EXECUTABLETYPE_TENSOR_HPP

#include <Common/ExecutableType/Array.hpp>
#include <Common/ExecutableType/NESType.hpp>
#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace NES::ExecutableTypes {

/**
 * @brief Container for fixed size tensors of primitive types.
 * @tparam T type of elements that belong to tensor
 * @tparam numberOfDimensions number of dimensions
 * @tparam totalSize total size of flattened tensor
 * @tparam dimensions size of each dimension
 */
template<typename T, std::size_t numberOfDimensions, std::size_t totalSize, std::size_t... dimensions>
class TensorBase : public std::array<T, totalSize>, public NESType {
  public:
    /**
     * @brief Public, externally visible type of this tensor
     */
    using type = T;
    /**
     * @brief Public, externally visible shape of this tensor
     */
    std::array<size_t, numberOfDimensions> shape;

    /**
     * @brief typedef for tensor
     */
    typedef std::array<T, totalSize> tensor;

    /**
     * @brief base constructor for tensor without init
     */
    TensorBase() {
        create(1, dimensions...);
        checkTotalSizeSameAsDims();
    }

    /**
     * @brief Construct from arguments which can be used to construct the underlying std::array `J...`.
     * Modified version of same Constructor from ArrayBase.hpp, part of code taken from there
     */
    template<typename... J, typename = std::enable_if_t<std::is_constructible_v<std::array<T, totalSize>, std::decay_t<J>...>>>
    inline TensorBase(J&&... f) noexcept(false)
        : std::array<T, totalSize>(std::forward<J>(f)...) {
        create(1, dimensions...);
        checkTotalSizeSameAsDims();
        this->runtimeConstructionTest();
    }

    /**
     * @brief Construct from `size` values of type T.
     * @tparam J
     * @param f variables used for construction
     * Modified version of same Constructor from ArrayBase.hpp, part of code taken from there
     * @throws std::runtime_error if the vector's size does not match `totalSize`.
     */
    template<typename... J,
             typename = std::enable_if_t<std::conjunction_v<std::is_same<T, std::decay_t<J>>...> && sizeof...(J) <= totalSize
                                         && (std::is_same_v<T, char> || sizeof...(J) == totalSize)>>
    inline TensorBase(J... f) noexcept(false)
        : std::array<T, totalSize>({std::forward<J>(f)...}) {
        create(1, dimensions...);
        checkTotalSizeSameAsDims();
        if constexpr (sizeof...(J) == totalSize) {
            this->runtimeConstructionTest();
        }
    }

    /**
     * @brief Construct from c-style array `val`.
     * Modified version of same Constructor from ArrayBase.hpp, part of code taken from there
     * @throws std::runtime_error if the vector's size does not match `totalSize`.
     */
    constexpr TensorBase(T const (&val)[totalSize]) noexcept(false)
        : std::array<T, totalSize>() {
        create(1, dimensions...);
        checkTotalSizeSameAsDims();
        this->initializeFrom(val, totalSize, std::make_integer_sequence<std::size_t, totalSize>());
        runtimeConstructionTest();
    }

    /***
     * @brief Construct from std::vector `vec`.
     *
     * @throws std::runtime_error if the vector's size does not match `totalSize`.
     * Modified version of same Constructor from ArrayBase.hpp, part of code taken from there
     */
    inline TensorBase(std::vector<T> const& vec) noexcept(false) : std::array<T, totalSize>() {
        create(1, dimensions...);
        checkTotalSizeSameAsDims();
        this->initializeFrom(vec, vec.size(), std::make_integer_sequence<std::size_t, totalSize>());
        runtimeConstructionTest();
    }

    /**
     * @brief () operator for indexing and obtaining the correct tensor values
     * @tparam Dims
     * @param dims inputted dimensions vars where we want to obtain the saved value for
     * @return obtain saved value at given index
     */
    template<typename... Dims>
    T operator()(Dims... dims) const {
        if (sizeof...(dims) != numberOfDimensions) {
            throw std::runtime_error("Dimensions and total size do not match.");
        }
        return tensor::at(getIndex(1, dims...));
    }

  protected:
    /***
     * @brief Protected constructor which is usable by partial specializations of the base class.
     * @tparam E
     * @param v
     * @param pSize
     * @throws std::runtime_error if the reported size `pSize` does not match `size`.
     * Code taken from ArrayBase.hpp
     */
    template<bool conductConstructionTest, typename E>
    constexpr TensorBase(E const* v, std::size_t pSize, std::integral_constant<bool, conductConstructionTest>) noexcept(false)
        : std::array<T, totalSize>() {
        this->initializeFrom(v, pSize, std::make_integer_sequence<std::size_t, totalSize>());
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
     * Code taken from ArrayBase.hpp
     */
    template<typename E, std::size_t... is>
    constexpr void
    initializeFrom(E const& payload, std::size_t pSize, std::integer_sequence<std::size_t, is...>) noexcept(false) {

        // Chars are allowed to be of smaller size than
        if constexpr (std::is_same_v<T, char>) {
            if (pSize > totalSize) {
                throw std::runtime_error("The initializer passed to NES::ArrayType is too long for the contained type.");
            }
            std::memcpy(&(this[0]), &(payload[0]), pSize);
        } else {
            if (pSize != totalSize) {
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
     * Code taken from ArrayBase.hpp
     */
    template<std::size_t i, typename E, typename = std::enable_if_t<i<totalSize>> constexpr void init(E const& v) noexcept {
        (*this)[i] = v[i];
    }

    /**
     * @brief method for creating a dense tensor
     * @tparam Dims template param, may be any integer, long, float or double value
     * @param currentDim the dimension we are currently looking at
     * @param sizeCurrentDim the size of said dimension
     * @param remainingDims remaining dimension sizes
     */
    template<typename... Dims>
    void create(size_t currentDim, size_t sizeCurrentDim, Dims... remainingDims) {
        shape[currentDim - 1] = sizeCurrentDim;
        create(currentDim + 1, remainingDims...);
    }
    /**
     * @brief method for creating a dense tensor from an underlying array
     */
    void create(size_t) {}

    /**
     * @brief computes the index that we want to access
     * @param currentDim the dimension we're currently obtaining the index from
     * @return index
     */
    [[nodiscard]] int computeIndex(size_t currentDim) const {
        if (currentDim == numberOfDimensions) {
            return 1;
        }
        return shape[currentDim] * computeIndex(currentDim + 1);
    }
    /**
     * @brief obtain index from inputted indices
     * @tparam Dims template param, may be any integer, long, float or double value
     * @param currentDim the dimension we are currently looking at
     * @param sizeCurrentDim the size of said dimension
     * @param remainingDims remaining dimension sizes
     * @return index that we want to access
     */
    template<typename... Dims>
    [[nodiscard]] int getIndex(size_t currentDim, size_t sizeCurrentDim, Dims... remainingDims) const
        noexcept(noexcept(sizeCurrentDim < shape[currentDim])) {
        return sizeCurrentDim * computeIndex(currentDim) + getIndex(currentDim + 1, remainingDims...);
    }
    /**
     * @brief last recursion step for index obtaining
     * @param currentDim
     * @return always returns 0
     */
    [[nodiscard]] int getIndex(size_t) const { return 0; }

    /**
     * @brief checks if total size is the same as size of the individual dimensions
     */
    inline void checkTotalSizeSameAsDims() const {
        size_t size = 1;
        for (auto dim : shape) {
            size *= dim;
        }
        if (size != totalSize) {
            throw std::out_of_range("Dimensions and total size do not match.");
        }
    }

    /**
     * @brief Conduct Runtime tests that ensure that the datatype is constructed.
     * Code taken from Array.hpp
     */
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
 * @brief Default Tensor template that defaults to TensorBase, used to easier create a tensor
 * @tparam T the type of elements that belong to the tensor
 * @tparam totalSize the fixed-size tensor's size
 * @tparam dimensions the size of each dimension
 */
template<typename T, std::size_t totalSize, std::size_t... dimensions>
class Tensor : public TensorBase<T, sizeof...(dimensions), totalSize, dimensions...> {
  public:
    using TensorBase<T, sizeof...(dimensions), totalSize, dimensions...>::TensorBase;
};

}// namespace NES::ExecutableTypes

#endif//NES__INCLUDE_COMMON_EXECUTABLETYPE_TENSOR_HPP
