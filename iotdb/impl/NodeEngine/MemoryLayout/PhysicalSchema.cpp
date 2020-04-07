#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalFieldUtil.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

namespace NES {

PhysicalSchema::PhysicalSchema(SchemaPtr _schema) {
  schema = std::make_shared<Schema>();
  schema->copyFields(_schema);
};

PhysicalSchemaPtr PhysicalSchema::createPhysicalSchema(SchemaPtr schema) {
    return std::make_shared<PhysicalSchema>(schema);
}

PhysicalFieldPtr PhysicalSchema::createPhysicalField(uint64_t fieldIndex, uint64_t bufferOffset) {
    assert(validFieldIndex(fieldIndex));
    auto logicalFieldsInSchema = schema->fields;
    auto logicalField = logicalFieldsInSchema[fieldIndex];
    return PhysicalFieldUtil::createPhysicalField(logicalField->getDataType(), bufferOffset);
}

uint64_t PhysicalSchema::getFieldOffset(uint64_t fieldIndex) {
    assert(validFieldIndex(fieldIndex));
    uint64_t offset = 0;
    for (uint64_t index = 0; index < fieldIndex; index++) {
        offset += schema->fields[index]->getFieldSize();
    }
    return offset;
}

bool PhysicalSchema::validFieldIndex(uint64_t fieldIndex) {
    auto fields = schema->fields;
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
