#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

namespace NES {

PhysicalSchema::PhysicalSchema(const SchemaPtr schema) : schema(schema) {
    for (auto field : schema->fields) {
        auto dataType = field->data_type;
        this->fields.push_back(this->createPhysicalField(dataType));
    };
};

PhysicalFieldPtr PhysicalSchema::createPhysicalField(const NES::DataTypePtr dataType) {
    if (dataType->isEqual(createDataType(UINT8))) {
        const auto field = new BasicPhysicalField<uint8_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(UINT16))) {
        auto field = new BasicPhysicalField<uint16_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(UINT32))) {
        auto field = new BasicPhysicalField<uint32_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(UINT64))) {
        auto field = new BasicPhysicalField<uint64_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(INT8))) {
        auto field = new BasicPhysicalField<int8_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(INT16))) {
        auto field = new BasicPhysicalField<int16_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(INT32))) {
        auto field = new BasicPhysicalField<int32_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(INT64))) {
        auto field = new BasicPhysicalField<int64_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(FLOAT32))) {
        auto field = new BasicPhysicalField<uint32_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(FLOAT64))) {
        auto field = new BasicPhysicalField<uint64_t>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(BOOLEAN))) {
        auto field = new BasicPhysicalField<bool>();
        return field->copy();
    } else if (dataType->isEqual(createDataType(CHAR))) {
        auto field = new BasicPhysicalField<char>();
        return field->copy();
    } else if (dataType->isArrayDataType()) {
        // TODO we have to introduce a proper physical field to support arrays in the future.
        auto field = new BasicPhysicalField<char*>();
        return field->copy();
    }
    NES_FATAL_ERROR("No physical field mapping for " << dataType->toString() << " available");
    NES_NOT_IMPLEMENTED;
}

PhysicalSchemaPtr PhysicalSchema::createPhysicalSchema(SchemaPtr schema) {
    return std::make_shared<PhysicalSchema>(schema);
}

PhysicalFieldPtr PhysicalSchema::getField(uint64_t fieldIndex) {
    return fields[fieldIndex];
}

uint64_t PhysicalSchema::getFieldOffset(uint64_t fieldIndex) {
    uint64_t offset = 0;
    for (uint64_t index = 0; index < fieldIndex; index++) {
        offset += this->schema->fields[index]->getFieldSize();
    }
    return offset;
}

uint64_t PhysicalSchema::getRecordSize() {
    return this->schema->getSchemaSize();
}
}
