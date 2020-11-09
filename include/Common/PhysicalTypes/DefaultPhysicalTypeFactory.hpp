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

#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_

#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>

namespace NES {

class Integer;
typedef std::shared_ptr<Integer> IntegerPtr;

class Array;
typedef std::shared_ptr<Array> ArrayPtr;

class Float;
typedef std::shared_ptr<Float> FloatPtr;

class Char;
typedef std::shared_ptr<Char> CharPtr;

class FixedChar;
typedef std::shared_ptr<FixedChar> FixedCharPtr;

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
    PhysicalTypePtr getPhysicalType(IntegerPtr integerType);

    /**
    * @brief Translates a char data type into a corresponding physical type.
    * @param dataType
    * @return PhysicalTypePtr
    */
    PhysicalTypePtr getPhysicalType(CharPtr charType);
    /**
    * @brief Translates a fixed char data type into a corresponding physical type.
    * @param fixedCharType
    * @return PhysicalTypePtr
    */
    PhysicalTypePtr getPhysicalType(FixedCharPtr fixedCharType);

    /**
    * @brief Translates a fixed char data type into a corresponding physical type.
    * @param floatType
    * @return PhysicalTypePtr
    */
    PhysicalTypePtr getPhysicalType(FloatPtr floatType);

    /**
    * @brief Translates a array data type into a corresponding physical type.
    * @param arrayType
    * @return PhysicalTypePtr
    */
    PhysicalTypePtr getPhysicalType(ArrayPtr arrayType);
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
