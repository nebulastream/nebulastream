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

template<typename T, std::size_t numberOfDimensions, std::size_t totalSize, typename = std::enable_if_t<!std::is_pointer_v<T> && std::is_arithmetic_v<T>>>
class Tensor : public Array<T, numberOfDimensions> {
  public:
    /**
     * Public, externally visible type of this tensor
     */
    using type = T;
    /**
     * @brief Public, externally visible shape of this tensor
     */
    size_t shape[numberOfDimensions];
    /**
     * @brief default constructor
     */
    Tensor() = default;

    /**
     * @brief: constructor for tensor
     * @tparam Dims
     * @param dims inputted dimensions for constructing the tensor
     */
    template<typename... Dims>
    Tensor(Dims... dims) {
        create(1, dims...);
    }
    /**
     * @brief [] operator for indexing and obtaining the correct tensor values
     * @tparam Dims
     * @param dims inputted dimensions vars where we want to obtain the saved value for
     * @return obtain saved value at given index
     */
    template<typename... Dims>
    T& operator[](Dims... dims) const {
        return tensor->at(getIndex(1, dims...));
    }

  private:
    /**
     * @brief private container for allocated tensor memory
     */
    std::shared_ptr<Array<T, totalSize>> tensor;

    /**
     * @brief total size for the underlying array
     */
    static constexpr size_t realSize = totalSize;

    /**
     * @brief method for creating a dense tensor
     * @tparam Dims
     * @param currentDim
     * @param sizeCurrentDim
     * @param remainingDims
     */
    template<typename... Dims>
    void create(size_t currentDim, size_t sizeCurrentDim, Dims... remainingDims) {
        shape[currentDim - 1] = sizeCurrentDim;
        create(currentDim + 1, remainingDims...);
    }
    /**
     * @brief method for creating a dense tensor from an underlying array
     * @param currentDimension
     */
    void create(size_t currentDimension) {
        tensor = std::make_shared<Array<T, realSize>>();
    }
    /**
     * @brief
     * @param currentDim
     * @return
     */
    [[nodiscard]] int computeIndex(size_t currentDim) const{
        if(currentDim == numberOfDimensions){
            return 1;
        }
        return shape[currentDim] * computeIndex(currentDim+1);
    }
    /**
     *
     * @tparam Dims
     * @param currentDim
     * @param sizeCurrentDim
     * @param remainingDims
     * @return
     */
    template<typename... Dims>
    [[nodiscard]] int getIndex(size_t currentDim, size_t sizeCurrentDim, Dims... remainingDims) const{
        return sizeCurrentDim * computeIndex(currentDim) + getIndex(currentDim + 1, remainingDims...);
    }
    /**
     *
     * @param currentDim
     * @return
     */
    [[nodiscard]] int getIndex(size_t currentDim) const {
        return 0;
    }

};

}// namespace NES::ExecutableTypes

#endif//NES_TENSOR_HPP
