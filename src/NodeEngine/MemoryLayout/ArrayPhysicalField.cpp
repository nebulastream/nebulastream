
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <DataTypes/PhysicalTypes/PhysicalType.hpp>
#include <memory>
namespace NES {
ArrayPhysicalField::ArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset)
    : PhysicalField(bufferOffset), componentField(componentField) {}

ArrayPhysicalField::ArrayPhysicalField(ArrayPhysicalField* physicalField) : PhysicalField(physicalField), componentField(physicalField->componentField){};

const PhysicalFieldPtr ArrayPhysicalField::copy() const {
    return std::make_shared<ArrayPhysicalField>(*this);
}

std::shared_ptr<ArrayPhysicalField> ArrayPhysicalField::asArrayField() {
    return std::static_pointer_cast<ArrayPhysicalField>(this->shared_from_this());
}

std::shared_ptr<PhysicalField> ArrayPhysicalField::operator[](uint64_t arrayIndex) {
    auto offsetInArray = componentField->size() * arrayIndex;
    return PhysicalFieldUtil::createPhysicalField(componentField, bufferOffset + offsetInArray);
}

PhysicalFieldPtr createArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset) {
    return std::make_shared<ArrayPhysicalField>(componentField, bufferOffset);
}
}// namespace NES