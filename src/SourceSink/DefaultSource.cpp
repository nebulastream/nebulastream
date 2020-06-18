#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

DefaultSource::DefaultSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                             const uint64_t numbersOfBufferToProduce,
                             size_t frequency)
    : GeneratorSource(schema, bufferManager, queryManager, numbersOfBufferToProduce) {
    NES_DEBUG("DefaultSource:" << this << " creating");
    this->gatheringInterval = frequency;
}

std::optional<TupleBuffer> DefaultSource::receiveData() {
    // 10 tuples of size one
    NES_DEBUG("Source:" << this << " requesting buffer");

    auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("Source:" << this << " got buffer");
    size_t tupleCnt = 10;
    auto layout = createRowLayout(std::make_shared<Schema>(schema));

    auto fields = schema->fields;
    for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
        for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            auto value = 1;
            auto dataType = fields[fieldIndex]->getDataType();
            auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                    layout->getValueField<char>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                    layout->getValueField<uint8_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                    layout->getValueField<uint16_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                    layout->getValueField<uint32_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                    layout->getValueField<uint64_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                    layout->getValueField<int8_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                    layout->getValueField<int16_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                    layout->getValueField<int32_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                    layout->getValueField<int64_t>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                    layout->getValueField<float>(recordIndex, fieldIndex)->write(buf, value);
                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                    layout->getValueField<double>(recordIndex, fieldIndex)->write(buf, value);
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            } else {
                NES_DEBUG("This data source only generates data for numeric fields");
            }
        }
    }
    buf.setNumberOfTuples(tupleCnt);
    // TODO move this to trace
    NES_DEBUG("Source: Generated buffer with " << buf.getNumberOfTuples() << "/" << schema->getSchemaSizeInBytes()
                                               << "\n"
                                               << UtilityFunctions::prettyPrintTupleBuffer(buf, schema));
    return buf;
}

SourceType DefaultSource::getType() const {
    return DEFAULT_SOURCE;
}

}// namespace NES
