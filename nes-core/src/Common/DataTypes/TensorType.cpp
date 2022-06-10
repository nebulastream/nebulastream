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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/TensorType.hpp>
#include <iostream>

namespace NES {

bool TensorType::isEquals(DataTypePtr otherDataType) {
    std::cout << "Do you have a problem here?" << std::endl;
    if (otherDataType->isTensor()) {
        std::cout << "Do you have a problem here?1" << std::endl;
        auto const otherTensor = as<TensorType>(otherDataType);
        std::cout << "Do you have a problem here?2" << std::endl;
        //todo find out why I cannot use these components
        bool helper = shape == otherTensor->shape && component->isEquals(otherTensor->component)
            && tensorMemoryFormat == otherTensor->tensorMemoryFormat;
        std::cout << (helper ? "true" : "false") << std::endl;
        return shape == otherTensor->shape && component->isEquals(otherTensor->component)
            && tensorMemoryFormat == otherTensor->tensorMemoryFormat;
    }
    std::cout << "Do you return false?" << std::endl;
    return false;
}

DataTypePtr TensorType::join(DataTypePtr) { return DataTypeFactory::createUndefined(); }

std::string TensorType::toString() { return "Tensor"; }

}// namespace NES