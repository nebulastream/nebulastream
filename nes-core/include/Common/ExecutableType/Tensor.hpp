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

#ifndef NES_TENSOR_HPP
#define NES_TENSOR_HPP

#include <Common/ExecutableType/Array.hpp>
#include <Common/ExecutableType/NESType.hpp>
#include <memory>

namespace NES::ExecutableTypes {

//Todo: add checks: dimensions, out of bounds, etc.
//Todo: add iterators

template<typename T, std::size_t dimension = 1, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class Tensor : public NESType {
  public:
    /*
     * @brief Public, externally visible data type of this tensor
     */
    using type = T;

    /**
     * @brief default constructor
     */
    Tensor() = default;

    /**
     * constructor for a tensor
     * @tparam Dims
     * @param ds
     */
    template<typename... Dims>
    Tensor(Dims... ds) : size(1) {
        create(1, ds...);
    }

    template<typename... Dims>
    T& operator()(Dims... ds) const {
        return pointer[getIndex(1, ds...)];
    }
    size_t getSize(size_t s = 1) const { return shape[s - 1]; }

  private:
    /**
     * @brief private, points to the base of the multidimensional array
     */
    std::unique_ptr<T[]> pointer;

    /**
     * @brief non initialized shape of the array
     */
    std::size_t shape[dimension];

    /**
     * @brief non intiialized number of dimensions
     */
    std::size_t size;

    template<typename... Dims>
    void create(size_t s, size_t d, Dims... ds) {
        size *= d;
        shape[s - 1] = d;
        create(s + 1, ds...);
    }
    void create(size_t s) { pointer = std::unique_ptr<T[]>(new T[size]); }
    int computeSubSize(size_t s) const {
        if (s == dimension) {
            return 1;
        }
        return shape[s] * computeSubSize(s + 1);
    }

    template<typename... Dims>
    size_t getIndex(size_t s, size_t d, Dims... ds) const {
        return d * computeSubSize(s) + getIndex(s + 1, ds...);
    }
    size_t getIndex(size_t s) const { return 0; }
};

}// namespace NES::ExecutableTypes

#endif//NES_TENSOR_HPP
