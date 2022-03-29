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

#ifndef NES_TENSORPHYSICALTYPE_HPP
#define NES_TENSORPHYSICALTYPE_HPP

#include <Common/DataTypes/TensorType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

class TensorPhysicalType final : public PhysicalType {

  public:
    inline TensorPhysicalType(DataTypePtr dataType,
                              uint16_t shape,
                              PhysicalTypePtr component,
                              TensorMemoryFormat tensorMemoryFormat) noexcept
        : PhysicalType(std::move(dataType)), shape(std::move(shape)), physicalComponentType(std::move(component)), tensorMemoryFormat(tensorMemoryFormat) {}

    virtual ~TensorPhysicalType() = default;



    std::vector<uint16_t> shape;
    PhysicalTypePtr const physicalComponentType;
    TensorMemoryFormat tensorMemoryFormat;
};

}// namespace NES

#endif//NES_TENSORPHYSICALTYPE_HPP
