#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

namespace NES {

PhysicalSchema::PhysicalSchema(const SchemaPtr schema) : schema(schema) {
};

PhysicalSchemaPtr PhysicalSchema::createPhysicalSchema(SchemaPtr schema) {
    return std::make_shared<PhysicalSchema>(schema);
}

PhysicalFieldPtr PhysicalSchema::createField(uint64_t fieldIndex, uint64_t bufferOffset) {
    assert(validFieldIndex(fieldIndex));
    auto fields = this->schema->fields;
    auto field = fields[fieldIndex];
    return PhysicalFieldUtil::createPhysicalField(field->getDataType(), bufferOffset);
}

uint64_t PhysicalSchema::getFieldOffset(uint64_t fieldIndex) {
    assert(validFieldIndex(fieldIndex));
    uint64_t offset = 0;
    for (uint64_t index = 0; index < fieldIndex; index++) {
        offset += this->schema->fields[index]->getFieldSize();
    }
    return offset;
}

bool PhysicalSchema::validFieldIndex(uint64_t fieldIndex) {
    auto fields = this->schema->fields;
    if (fieldIndex > fields.size()) {
        NES_FATAL_ERROR(
            "PhysicalSchema: field index " << fieldIndex << " is out of bound. Schema only contains " << fields.size()
                                           << " fields.")
        throw IllegalArgumentException("Field index out of bound");
    }
    return true;
}

uint64_t PhysicalSchema::getRecordSize() {
    return this->schema->getSchemaSize();
}
}
