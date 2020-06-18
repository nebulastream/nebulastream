
#include <DataTypes/PhysicalTypes/BasicPhysicalType.hpp>
#include <DataTypes/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <memory>
#include <utility>
namespace NES {
ArrayPhysicalField::ArrayPhysicalField(PhysicalTypePtr componentField, uint64_t bufferOffset)
    : PhysicalField(bufferOffset), componentField(std::move(componentField)) {}

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