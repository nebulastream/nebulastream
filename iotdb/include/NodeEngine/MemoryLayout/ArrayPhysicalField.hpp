#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
namespace NES {
class ArrayPhysicalField : public PhysicalField {
  public:
    ArrayPhysicalField(DataTypePtr componentField, uint64_t bufferOffset);
    ~ArrayPhysicalField() {};
    ArrayPhysicalField(ArrayPhysicalField* physicalField);
    std::shared_ptr<ArrayPhysicalField> asArrayField() override;
    const PhysicalFieldPtr copy() const override;
    std::shared_ptr<PhysicalField> operator[](uint64_t arrayIndex);
  private:
    uint64_t bufferOffset;
    DataTypePtr componentField;
};
}

#endif //NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
