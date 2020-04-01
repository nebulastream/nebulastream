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
#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/SenseSource.hpp>

namespace NES {

const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(
    SchemaPtr schema) {
  return std::make_shared<DefaultSource>(schema, /*bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(
    SchemaPtr schema, size_t numbersOfBufferToProduce, size_t frequency) {
  return std::make_shared<DefaultSource>(schema, numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer() {
  return std::make_shared<DefaultSource>(
      SchemaTemp::create()->addField(createField("id", UINT64)), /**bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForVarBuffers(
    size_t numbersOfBufferToProduce, double frequency) {
  return std::make_shared<DefaultSource>(
      SchemaTemp::create()->addField(createField("id", UINT64)),
      numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createZmqSource(SchemaPtr schema,
                                    const std::string &host,
                                    const uint16_t port) {
  return std::make_shared<ZmqSource>(schema, host, port);
}

const DataSourcePtr createBinaryFileSource(SchemaPtr schema,
                                           const std::string &path_to_file) {
  return std::make_shared<BinarySource>(schema, path_to_file);
}

const DataSourcePtr createSenseSource(SchemaPtr schema,
                                      const std::string& udfs) {
  return std::make_shared<SenseSource>(schema, udfs);
}

const DataSourcePtr createCSVFileSource(SchemaPtr schema,
                                        const std::string &pathToFile,
                                        const std::string &delimiter,
                                        size_t numbersOfBufferToProduce,
                                        size_t frequency) {
  return std::make_shared<CSVSource>(schema, pathToFile, delimiter,
                                     numbersOfBufferToProduce, frequency);
}

}

