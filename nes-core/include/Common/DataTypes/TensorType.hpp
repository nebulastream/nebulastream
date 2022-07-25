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

#ifndef NES_INCLUDE_COMMON_DATATYPES_TENSOR_HPP
#define NES_INCLUDE_COMMON_DATATYPES_TENSOR_HPP

#include <Common/DataTypes/DataType.hpp>
#include <vector>
#include <iostream>

namespace NES {

enum TensorMemoryFormat { DENSE };

/**
 * @brief constructs a dense tensor
 */
class TensorType : public DataType {

  public:
    /**
     * @brief constructs a new Tensor
     * @param shape gives the shape of the tensor, i.e. the number of elements in each dimension where the position i of the int in the vector describes the size of ith
     * dimension, the length of the vector gives the rank of the tensor, i.e. the number of dimensions of a tensor (terminology confirms to tensorflow's terminology)
     * @param component data type of the entries in tensor, currently limited to numeric data types
     * @param tensorMemoryFormat the type of underlying data structure for saving in memory, the tensor should use
     */
    inline TensorType(std::vector<std::size_t> shape, DataTypePtr component, TensorMemoryFormat tensorType) noexcept
        : shape(std::move(shape)), component(std::move(component)), tensorMemoryFormat(tensorType) {
        for (auto dimension : this->shape){
            totalSize *= dimension;
        }
    }

    virtual ~TensorType() = default;

    /**
     * @brief Checks if this data type is a tensor
     * @return return true, if it is a tensor
     */
    [[nodiscard]] bool isTensor() const final { return true; }

    /**
     * @brief Checks if two data types are equal
     * @param otherDataType
     * @return true if equal
     */
    bool isEquals(DataTypePtr otherDataType) final;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    DataTypePtr join(DataTypePtr otherDataType) final;

    /**
    * @brief Returns a string representation of the data type.
    * @return string
    */
    std::string toString() final;

    const std::vector<std::size_t> shape = {};
    DataTypePtr const component;
    TensorMemoryFormat const tensorMemoryFormat;
    size_t totalSize = 1;
};

}// namespace NES

#endif//NES_INCLUDE_COMMON_DATATYPES_TENSOR_HPP
