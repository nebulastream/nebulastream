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

class DefaultPhysicalTypeFactory : public PhysicalTypeFactory {
  public:
    DefaultPhysicalTypeFactory();
    PhysicalTypePtr getPhysicalType(DataTypePtr dataType) override;

  private:
    PhysicalTypePtr getPhysicalType(IntegerPtr integerType);
    PhysicalTypePtr getPhysicalType(CharPtr charType);
    PhysicalTypePtr getPhysicalType(FixedCharPtr charType);
    PhysicalTypePtr getPhysicalType(FloatPtr floatType);
    PhysicalTypePtr getPhysicalType(ArrayPtr arrayType);
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
