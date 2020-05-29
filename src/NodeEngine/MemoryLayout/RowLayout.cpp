#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <utility>

namespace NES {

RowLayout::RowLayout(PhysicalSchemaPtr physicalSchema) : MemoryLayout(std::move(physicalSchema)) {}

uint64_t RowLayout::getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) {
    return this->physicalSchema->getFieldOffset(fieldIndex) + (this->physicalSchema->getRecordSize() * recordIndex);
}

MemoryLayoutPtr RowLayout::copy() const {
    return std::make_shared<RowLayout>(*this);
}

MemoryLayoutPtr createRowLayout(SchemaPtr schema) {
    auto physicalSchema = PhysicalSchema::createPhysicalSchema(schema);
    return std::make_shared<RowLayout>(physicalSchema);
};

}// namespace NES