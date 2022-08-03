#ifndef NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
#define NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
#include "DataSource.hpp"
#include <Catalogs/Source/PhysicalSourceTypes/JSONSourceType.hpp>
#include <simdjson.h>
#ifdef ENABLE_SIMDJSON_BUILD

namespace NES {

class JSONParser;
using JSONParserPtr = std::shared_ptr<JSONParser>;
/**
 * @brief this class implements a JSON input source
 */
class JSONSource : public DataSource {
  public:
    explicit JSONSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        JSONSourceTypePtr jsonSourceType,
                        OperatorId operatorId,
                        OriginId originId,
                        size_t numSourceLocalBuffers,
                        GatheringMode::Value gatheringMode);

    std::optional<Runtime::TupleBuffer> receiveData() override;
    std::string toString() const override;
    SourceType getType() const override;

    /**
     * @brief Get file path for the JSON file
     */
    std::string getFilePath() const;

    /**
     * @brieg Fill Buffer with JSON data from file
     */
    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer);

    /**
     * @brief getter for source config
     * @return JSONSourceType
     */
    const JSONSourceTypePtr& getSourceConfig() const;

  protected:
    simdjson::dom::parser parser;

  private:
    JSONParserPtr inputParser;
    simdjson::padded_string json;
    std::vector<PhysicalTypePtr> physicalTypes;
    JSONSourceTypePtr jsonSourceType;
    std::string filePath;
    uint32_t numBuffersToProcess;
};
using JSONSourcePtr = std::shared_ptr<JSONSource>;
}//namespace NES

#endif//ENABLE_SIMDJSON_BUILD
#endif//NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
