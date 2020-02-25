#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

DefaultSource::DefaultSource(const Schema &schema,
                             const uint64_t numbersOfBufferToProduce,
                             size_t frequency)
    :
    GeneratorSource(schema, numbersOfBufferToProduce) {
  this->gatheringInterval = frequency;
}

TupleBufferPtr DefaultSource::receiveData() {
  // 10 tuples of size one
  TupleBufferPtr buf = BufferManager::instance().getBuffer();
  size_t tupleCnt = 10;
  auto layout = createRowLayout(std::make_shared<Schema>(schema));

  assert(buf->getBuffer() != NULL);

  for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
    for (uint64_t fieldIndex = 0; fieldIndex < this->schema.getSize();
        fieldIndex++) {
      layout->writeField<uint64_t>(buf, recordIndex, fieldIndex, 1);
    }
  }
  buf->setTupleSizeInBytes(sizeof(uint64_t));
  buf->setNumberOfTuples(tupleCnt);
  return buf;
}
}
BOOST_CLASS_EXPORT(NES::DefaultSource);
