#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_

#include <DataTypes/PhysicalTypes/PhysicalType.hpp>
namespace NES {

class BasicPhysicalType : public PhysicalType {
  public:
    enum NativeType {
        UINT_8,
        UINT_16,
        UINT_32,
        UINT_64,
        INT_8,
        INT_16,
        INT_32,
        INT_64,
        FLOAT,
        DOUBLE,
        CHAR,
        BOOLEAN
    };
    BasicPhysicalType(DataTypePtr type, NativeType nativeType);
    static PhysicalTypePtr create(DataTypePtr type, NativeType nativeType);

    bool isBasicType() override;
    u_int8_t size() const override;
    NativeType getNativeType();

    std::string convertRawToString(void* rawData) override;

  private:
    const NativeType nativeType;
};

typedef std::shared_ptr<BasicPhysicalType> BasicPhysicalTypePtr;

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
