#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

DefaultSource::DefaultSource(const Schema& schema,
                             const uint64_t numbersOfBufferToProduce,
                             size_t frequency)
    :
    GeneratorSource(schema, numbersOfBufferToProduce) {
    this->gatheringInterval = frequency;
}

TupleBufferPtr DefaultSource::receiveData() {
    // 10 tuples of size one
    TupleBufferPtr buf = BufferManager::instance().getFixedSizeBuffer();
    size_t tupleCnt = 10;
    auto layout = createRowLayout(std::make_shared<Schema>(schema));

    assert(buf->getBuffer() != nullptr);
    auto fields = schema.fields;
    for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
        for (uint64_t fieldIndex = 0; fieldIndex < fields.size();
             fieldIndex++) {
            auto value = 1;
            auto dataType = fields[fieldIndex]->getDataType();
            if (dataType->isEqual(createDataType(UINT8))) {
                layout->getValueField<uint8_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(UINT16))) {
                layout->getValueField<uint16_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(UINT32))) {
                layout->getValueField<uint32_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(UINT64))) {
                layout->getValueField<uint64_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(INT8))) {
                layout->getValueField<int8_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(INT16))) {
                layout->getValueField<int16_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(INT32))) {
                layout->getValueField<int32_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(INT64))) {
                layout->getValueField<int64_t>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(FLOAT32))) {
                layout->getValueField<float>(recordIndex, fieldIndex)->write(buf, value);
            } else if (dataType->isEqual(createDataType(FLOAT64))) {
                layout->getValueField<double>(recordIndex, fieldIndex)->write(buf, value);
            }else{
                NES_DEBUG("This data source only generates data for numeric fields")
            }

        }
    }
    buf->setTupleSizeInBytes(schema.getSchemaSize());
    buf->setNumberOfTuples(tupleCnt);
    return buf;
}
}
BOOST_CLASS_EXPORT(NES::DefaultSource);
