#ifndef NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
#define NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
#include "DataSource.hpp"
#ifdef ENABLE_SIMDJSON_BUILD

namespace NES {
class JSONSource : public DataSource {
  public:
    explicit JSONSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        OperatorId operatorId,
                        OriginId originId,
                        size_t numSourceLocalBuffers,
                        GatheringMode::Value gatheringMode);

    std::optional<Runtime::TupleBuffer> receiveData() override;
    std::string toString() const override;
    SourceType getType() const override;
    std::tuple<int64_t, std::string> parse(const std::string& filepath);
};
}//namespace NES

#endif//ENABLE_SIMDJSON_BUILD
#endif//NES_NES_CORE_INCLUDE_SOURCES_JSONSOURCE_H_
