#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_

#include <DataTypes/PhysicalTypes/PhysicalTypeFactory.hpp>

namespace NES{

class Integer;
typedef std::shared_ptr<Integer> IntegerPtr;

class Array;
typedef std::shared_ptr<Array> ArrayPtr;

class Float;
typedef std::shared_ptr<Float> FloatPtr;

class DefaultPhysicalTypeFactory: public PhysicalTypeFactory{
  public:
    DefaultPhysicalTypeFactory();
    PhysicalTypePtr getPhysicalType(DataTypePtr dataType) override;

  private:
    PhysicalTypePtr getPhysicalType(IntegerPtr integerType);
    PhysicalTypePtr getPhysicalType(FloatPtr floatType);
    PhysicalTypePtr getPhysicalType(ArrayPtr arrayType);
};


}

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_DEFAULTPHYSICALTYPEFACTORY_HPP_
