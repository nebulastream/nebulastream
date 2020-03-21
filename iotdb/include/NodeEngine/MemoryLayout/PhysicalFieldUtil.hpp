#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
#include <memory>

namespace NES {

class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class PhysicalFieldUtil {
  public:
    static PhysicalFieldPtr createPhysicalField(const DataTypePtr dataType, uint64_t bufferOffset);
};
}

#endif //NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELDUTIL_HPP_
