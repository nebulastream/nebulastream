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

#include <Common/ExecutableType/NESType.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <vector>
#include <array>

namespace NES::ExecutableTypes {

template<typename T, std::size_t _N_dims, std::size_t _N_vals, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class TensorBase : public NESType {
  public:
    /*
     * @brief Public, externally visible data type of this tensor
     */
    using type = T;
    /*
     * @brief Public, externally visible number of dimensions of tensor
     */
    static constexpr size_t numberOfDimensions = _N_dims;
    /*
     * @brief Public, externally visible number of values
     */
    static constexpr size_t numberOfValues = _N_vals;

  private:
};

/**
 * @brief        Default ArrayType template which defaults to ArrayBase.
 *
 * @tparam T     the type of the elements that belong to the array
 * @tparam size  the fixed-size array's size.
 *
 * @see TensorBase<T, size>
 */
template<typename T, std::size_t numberOfDimensions, std::size_t numberOfValues, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class Tensor final : public TensorBase<T, numberOfDimensions, numberOfValues> {
  public:
    using TensorBase<T, numberOfDimensions, numberOfValues>::TensorBase;
};

}// namespace NES::ExecutableTypes

#endif//NES_TENSOR_HPP
