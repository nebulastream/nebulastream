#include <API/Types/DataTypes.hpp>
#include <NodeEngine/MemoryLayout/BasicPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <QueryCompiler/DataTypes/ArrayDataType.hpp>
#include <Util/Logger.hpp>

namespace NES {

class ArrayDataType;

std::shared_ptr<PhysicalField> PhysicalFieldUtil::createPhysicalField(const DataTypePtr dataType,
                                                                      uint64_t bufferOffset) {
    if (dataType->isEqual(createDataType(UINT8))) {
        return createBasicPhysicalField<uint8_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(UINT16))) {
        return createBasicPhysicalField<uint16_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(UINT32))) {
        return createBasicPhysicalField<uint32_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(UINT64))) {
        return createBasicPhysicalField<uint64_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(INT8))) {
        return createBasicPhysicalField<int8_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(INT16))) {
        return createBasicPhysicalField<int16_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(INT32))) {
        return createBasicPhysicalField<int32_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(INT64))) {
        return createBasicPhysicalField<int64_t>(bufferOffset);
    } else if (dataType->isEqual(createDataType(FLOAT32))) {
        return createBasicPhysicalField<float>(bufferOffset);
    } else if (dataType->isEqual(createDataType(FLOAT64))) {
        return createBasicPhysicalField<double>(bufferOffset);
    } else if (dataType->isEqual(createDataType(BOOLEAN))) {
        return createBasicPhysicalField<bool>(bufferOffset);
    } else if (dataType->isEqual(createDataType(CHAR))) {
        return createBasicPhysicalField<char>(bufferOffset);
    } else if (dataType->isArrayDataType()) {
        auto arrayType = std::dynamic_pointer_cast<ArrayDataType>(dataType);
        return createArrayPhysicalField(arrayType->getComponentDataType(), bufferOffset);
    } else {
        NES_FATAL_ERROR("No physical field mapping for " << dataType->toString() << " available");
        NES_NOT_IMPLEMENTED();
        ;
    }
}
}// namespace NES