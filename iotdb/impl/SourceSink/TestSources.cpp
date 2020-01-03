#include <SourceSink/BinarySource.hpp>
#include <SourceSink/CSVSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <SourceSink/ZmqSource.hpp>
#include <SourceSink/DataSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <SourceSink/SourceCreator.hpp>

namespace iotdb {

TupleBufferPtr OneGeneratorSource::receiveData(){
    // 10 tuples of size one
    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    size_t tupleCnt = 10;
    auto layout = createRowLayout(std::make_shared<Schema>(schema));

    assert(buf->getBuffer() != NULL);

    for (uint64_t recordIndex = 0; recordIndex < tupleCnt; recordIndex++) {
      for (uint64_t fieldIndex = 0; fieldIndex < this->schema.getSize(); fieldIndex++) {
        layout->writeField<uint64_t>(buf, recordIndex, fieldIndex, 1);
      }
    }
    buf->setTupleSizeInBytes(sizeof(uint64_t));
    buf->setNumberOfTuples(tupleCnt);
    return buf;
  }


const DataSourcePtr createTestDataSourceWithSchema(const Schema &schema) {
  return std::make_shared<OneGeneratorSource>(schema, 1);
}

const DataSourcePtr createTestSourceWithoutSchema() {
  return std::make_shared<OneGeneratorSource>(Schema::create().addField(createField("id", UINT64)), 1);
}

//const DataSourcePtr createYSBSource(size_t bufferCnt, size_t campaingCnt,
//                                    bool preGenerated) {
//
//  Schema schema =
//      Schema::create().addField("user_id", 16).addField("page_id", 16).addField(
//              "campaign_id", 16).addField("event_type", 16).addField("ad_type", 16)
//          .addField("current_ms", UINT64).addField("ip", INT32);
//
//  return std::make_shared<YSBGeneratorSource>(schema, bufferCnt, campaingCnt, preGenerated);
//}

const DataSourcePtr createZmqSource(const Schema &schema,
                                    const std::string &host,
                                    const uint16_t port) {
  return std::make_shared<ZmqSource>(schema, host, port);
}

const DataSourcePtr createBinaryFileSource(const Schema &schema,
                                           const std::string &path_to_file) {
  return std::make_shared<BinarySource>(schema, path_to_file);
}

const DataSourcePtr createCSVFileSource(const Schema &schema,
                                        const std::string &path_to_file) {
  return std::make_shared<CSVSource>(schema, path_to_file);
}

}

BOOST_CLASS_EXPORT(iotdb::OneGeneratorSource);
