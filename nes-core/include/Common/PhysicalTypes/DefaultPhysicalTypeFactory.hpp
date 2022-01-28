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

#ifndef NES_INCLUDE_COMMON_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
#define NES_INCLUDE_COMMON_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_

#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

namespace NES {

class Integer;
using IntegerPtr = std::shared_ptr<Integer>;

class ArrayType;
using ArrayPtr = std::shared_ptr<ArrayType>;

class Float;
using FloatPtr = std::shared_ptr<Float>;

class Char;
using CharPtr = std::shared_ptr<Char>;

/**
 * @brief This is a default physical type factory, which maps nes types to common x86 types.
 */
class DefaultPhysicalTypeFactory : public PhysicalTypeFactory {
  public:
    DefaultPhysicalTypeFactory();

    /**
     * @brief Translates a nes data type into a corresponding physical type.
     * @param dataType
     * @return PhysicalTypePtr
     */
    PhysicalTypePtr getPhysicalType(DataTypePtr dataType) override;

  private:
    /**
    * @brief Translates an integer data type into a corresponding physical type.
    * @param integerType
    * @return PhysicalTypePtr
    */
    static PhysicalTypePtr getPhysicalType(const IntegerPtr& integerType);

    /**
    * @brief Translates a char data type into a corresponding physical type.
    * @param dataType
    * @return PhysicalTypePtr
    */
    static PhysicalTypePtr getPhysicalType(const CharPtr& charType);

    /**
    * @brief Translates a fixed char data type into a corresponding physical type.
    * @param floatType
    * @return PhysicalTypePtr
    */
    static PhysicalTypePtr getPhysicalType(const FloatPtr& floatType);

    /**
    * @brief Translates a array data type into a corresponding physical type.
    * @param arrayType
    * @return PhysicalTypePtr
    */
    PhysicalTypePtr getPhysicalType(const ArrayPtr& arrayType);
};

}// namespace NES

#endif  // NES_INCLUDE_COMMON_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
