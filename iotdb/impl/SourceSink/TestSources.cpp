#include <SourceSink/BinarySource.hpp>
#include <SourceSink/CSVSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <SourceSink/ZmqSource.hpp>
#include <SourceSink/DataSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include "../../include/SourceSink/SourceCreator.hpp"
#include "../../include/YSB_legacy/YSBGeneratorSource.hpp"


namespace iotdb {

const DataSourcePtr createTestDataSourceWithSchema(const Schema& schema) {
  // Shall this go to the UnitTest Directory in future?
  class Functor {
   public:
    Functor()
        : one(1) {
    }
    TupleBufferPtr operator()() {
      // 10 tuples of size one
      TupleBufferPtr buf = BufferManager::instance().getBuffer();
      size_t tupleCnt = buf->getBufferSizeInBytes() / sizeof(uint64_t);

      assert(buf->getBuffer() != NULL);

      uint64_t* tuples = (uint64_t*) buf->getBuffer();
      for (uint64_t i = 0; i < tupleCnt; i++) {
        tuples[i] = one;
      }
      buf->setTupleSizeInBytes(sizeof(uint64_t));
      buf->setNumberOfTuples(tupleCnt);
      return buf;
    }

    uint64_t one;
  };
  // TODO: In the future add support for GeneratorSource serialization, for now this operator cannot be used anyways
  //DataSourcePtr source(new GeneratorSource<Functor>(schema, 1));
  DataSourcePtr source(new ZmqSource(schema, "", 0));

  return source;
}

const DataSourcePtr createTestSourceWithoutSchema() {
  // Shall this go to the UnitTest Directory in future?
  class Functor {
   public:
    Functor()
        : one(1) {
    }
    TupleBufferPtr operator()() {
      // 10 tuples of size one
      TupleBufferPtr buf = BufferManager::instance().getBuffer();
      size_t tupleCnt = buf->getBufferSizeInBytes() / sizeof(uint64_t);

      assert(buf->getBuffer() != NULL);

      uint64_t* tuples = (uint64_t*) buf->getBuffer();
      for (uint64_t i = 0; i < tupleCnt; i++) {
        tuples[i] = one;
      }
      buf->setTupleSizeInBytes(sizeof(uint64_t));
      buf->setNumberOfTuples(tupleCnt);
      return buf;
    }

    uint64_t one;
  };

  DataSourcePtr source(
      new GeneratorSource<Functor>(
          Schema::create().addField(createField("id", UINT32)), 1));

  return source;
}

const DataSourcePtr createYSBSource(size_t bufferCnt, size_t campaingCnt,
                                    bool preGenerated) {

  Schema schema =
      Schema::create().addField("user_id", 16).addField("page_id", 16).addField(
          "campaign_id", 16).addField("event_type", 16).addField("ad_type", 16)
          .addField("current_ms", UINT64).addField("ip", INT32);


  return std::make_shared<YSBGeneratorSource>(schema, bufferCnt, campaingCnt, preGenerated);
}

const DataSourcePtr createZmqSource(const Schema& schema,
                                    const std::string& host,
                                    const uint16_t port) {
  return std::make_shared<ZmqSource>(schema, host, port);
}

const DataSourcePtr createBinaryFileSource(const Schema& schema,
                                           const std::string& path_to_file) {
  return std::make_shared<BinarySource>(schema, path_to_file);
}

const DataSourcePtr createCSVFileSource(const Schema& schema,
                                        const std::string& path_to_file) {
  return std::make_shared<CSVSource>(schema, path_to_file);
}

}
