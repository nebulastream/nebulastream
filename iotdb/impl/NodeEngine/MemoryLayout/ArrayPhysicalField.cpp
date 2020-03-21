
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <memory>
namespace NES {
ArrayPhysicalField::ArrayPhysicalField(DataTypePtr componentField, uint64_t bufferOffset)
    : componentField(componentField), bufferOffset(bufferOffset) {}

const PhysicalFieldPtr ArrayPhysicalField::copy() const {
    return std::static_pointer_cast<PhysicalField>(std::make_shared<ArrayPhysicalField>(*this));
}

std::shared_ptr<ArrayPhysicalField> ArrayPhysicalField::asArrayField() {
    return std::static_pointer_cast<ArrayPhysicalField>(this->shared_from_this());
}
ArrayPhysicalField::ArrayPhysicalField(ArrayPhysicalField* physicalField) {
    this->componentField = physicalField->componentField;
    this->bufferOffset = physicalField->bufferOffset;
};

std::shared_ptr<PhysicalField> ArrayPhysicalField::operator[](uint64_t arrayIndex) {
    auto offsetInArray = componentField->getSizeBytes()*arrayIndex;
    return PhysicalFieldUtil::createPhysicalField(componentField, bufferOffset + offsetInArray);
}

PhysicalFieldPtr createArrayPhysicalField(DataTypePtr componentField, uint64_t bufferOffset) {
    return std::make_shared<ArrayPhysicalField>(componentField, bufferOffset);
}
}