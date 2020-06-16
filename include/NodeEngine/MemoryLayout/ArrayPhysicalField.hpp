#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
namespace NES {

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

/**
 * @brief Represents an array field at a specific position in a memory buffer.
 */
class ArrayPhysicalField : public PhysicalField {
  public:
    ArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset);
    ~ArrayPhysicalField() = default;
    ArrayPhysicalField(ArrayPhysicalField* physicalField);
    /**
     * @brief Cast the PhysicalField into an ArrayField
     * @return std::shared_ptr<ArrayPhysicalField>
     */
    std::shared_ptr<ArrayPhysicalField> asArrayField() override;
    const PhysicalFieldPtr copy() const override;
    /**
     * @brief Get the PhysicalField at a specific array position.
     * @return std::shared_ptr<PhysicalField>
     */
    std::shared_ptr<PhysicalField> operator[](uint64_t arrayIndex);

  private:
    PhysicalTypePtr componentField;
};
}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_ARRAYPHYSICALFIELD_HPP_
