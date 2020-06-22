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
