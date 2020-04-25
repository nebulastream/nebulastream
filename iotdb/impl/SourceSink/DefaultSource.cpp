#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

DefaultSource::DefaultSource(SchemaPtr schema,
                             const uint64_t numbersOfBufferToProduce,
                             size_t frequency)
    :
    GeneratorSource(schema, numbersOfBufferToProduce) {
    NES_DEBUG("DefaultSource:" << this << " creating")
    this->gatheringInterval = frequency;
}

std::optional<TupleBuffer> DefaultSource::receiveData(BufferManagerPtr buffMgnr) {
    // 10 tuples of size one
    NES_DEBUG("Source:" << this << " requesting buffer")

    auto buf = buffMgnr->getBufferBlocking();
    NES_DEBUG("Source:" << this << " got buffer")
    size_t tupleCnt = 10;
    auto layout = createRowLayout(std::make_shared<Schema>(schema));

    auto fields = schema->fields;
    for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
        for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            auto value = 1;
            auto dataType = fields[fieldIndex]->getDataType();
            if (dataType->isEqual(createDataType(UINT8))) {
                layout->getValueField<uint8_t>(recordIndex, fieldIndex)->write(buf,
                                                                               value);
            } else if (dataType->isEqual(createDataType(UINT16))) {
                layout->getValueField<uint16_t>(recordIndex, fieldIndex)->write(buf,
                                                                                value);
            } else if (dataType->isEqual(createDataType(UINT32))) {
                layout->getValueField<uint32_t>(recordIndex, fieldIndex)->write(buf,
                                                                                value);
            } else if (dataType->isEqual(createDataType(UINT64))) {
                layout->getValueField<uint64_t>(recordIndex, fieldIndex)->write(buf,
                                                                                value);
            } else if (dataType->isEqual(createDataType(INT8))) {
                layout->getValueField<int8_t>(recordIndex, fieldIndex)->write(buf,
                                                                              value);
            } else if (dataType->isEqual(createDataType(INT16))) {
                layout->getValueField<int16_t>(recordIndex, fieldIndex)->write(buf,
                                                                               value);
            } else if (dataType->isEqual(createDataType(INT32))) {
                layout->getValueField<int32_t>(recordIndex, fieldIndex)->write(buf,
                                                                               value);
            } else if (dataType->isEqual(createDataType(INT64))) {
                layout->getValueField<int64_t>(recordIndex, fieldIndex)->write(buf,
                                                                               value);
            } else if (dataType->isEqual(createDataType(FLOAT32))) {
                layout->getValueField<float>(recordIndex, fieldIndex)->write(buf,
                                                                             value);
            } else if (dataType->isEqual(createDataType(FLOAT64))) {
                layout->getValueField<double>(recordIndex, fieldIndex)->write(buf,
                                                                              value);
            } else {
                NES_DEBUG("This data source only generates data for numeric fields")
            }

        }
    }
    buf.setTupleSizeInBytes(schema->getSchemaSizeInBytes());
    buf.setNumberOfTuples(tupleCnt);
    return buf;
}

SourceType DefaultSource::getType() const {
    return DEFAULT_SOURCE;
}

}
BOOST_CLASS_EXPORT(NES::DefaultSource);
