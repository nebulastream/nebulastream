#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
#include <memory>

namespace NES {

class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

/**
 * @brief Util class to create a PhysicalField for a specific data type and a offset in a buffer.
 */
class PhysicalFieldUtil {
  public:
    /**
     * @brief creates the corresponding PhysicalFieldPtr with respect to a particular data type.
     * @param dataType
     * @param bufferOffset offset in the underling buffer
     */
    static PhysicalFieldPtr createPhysicalField(const DataTypePtr dataType, uint64_t bufferOffset);
};
}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
