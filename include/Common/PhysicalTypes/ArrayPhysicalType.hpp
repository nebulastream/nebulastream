#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

class ArrayPhysicalType : public PhysicalType {

  public:
    ArrayPhysicalType(DataTypePtr type, uint64_t length, PhysicalTypePtr physicalComponentType);
    static PhysicalTypePtr create(DataTypePtr type, uint64_t length, PhysicalTypePtr component);
    bool isArrayType() override;
    u_int8_t size() const override;
    std::string convertRawToString(void* rawData) override;
    const PhysicalTypePtr& getPhysicalComponentType() const;
    const uint64_t getLength() const;
    std::string toString() override;

  private:
    const PhysicalTypePtr physicalComponentType;
    const uint64_t length;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
