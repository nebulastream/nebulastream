#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPEFACTORY_HPP_
#include <memory>
namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

class PhysicalTypeFactory {
  public:
   virtual PhysicalTypePtr getPhysicalType(DataTypePtr dataType);
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPEFACTORY_HPP_
