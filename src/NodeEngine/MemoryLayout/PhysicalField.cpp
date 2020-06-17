#include <NodeEngine/MemoryLayout/PhysicalField.hpp>

namespace NES {

PhysicalField::PhysicalField(uint64_t bufferOffset) : bufferOffset(bufferOffset){};

PhysicalField::~PhysicalField(){};

/**
 * @brief This function is only valid for ArrayPhysicalFields.
 * @return std::shared_ptr<ArrayPhysicalField>
 */
std::shared_ptr<ArrayPhysicalField> PhysicalField::asArrayField() {
    NES_FATAL_ERROR("This field is not an array field");
    throw IllegalArgumentException("This field is not an array field");
}
}// namespace NES