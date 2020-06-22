#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>
namespace NES {

/**
 * @brief The BasicPhysicalType represents nes data types, which can be directly mapped to a native c++ type.
 */
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

    /**
     * @brief Constructor for a basic physical type.
     * @param type the data type represented by this physical type
     * @param nativeType the native type of the nes type.
     */
    BasicPhysicalType(DataTypePtr type, NativeType nativeType);

    /**
     * @brief Factory function to create a new physical type.
     * @param type
     * @param nativeType
     * @return PhysicalTypePtr
     */
    static PhysicalTypePtr create(DataTypePtr type, NativeType nativeType);

    /**
     * @brief get the underling native data type for this physical data type.
     * @return NativeType
     */
    NativeType getNativeType();

    /**
     * @brief Indicates if this is a basic data type.
     * @return true
     */
    bool isBasicType() override;

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return u_int64_t
     */
    u_int64_t size() const override;


    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void* rawData) override;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    std::string toString() override;

  private:
    const NativeType nativeType;
};

typedef std::shared_ptr<BasicPhysicalType> BasicPhysicalTypePtr;

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
