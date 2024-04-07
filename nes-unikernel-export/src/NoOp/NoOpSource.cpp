#include <NoOp/NoOpSource.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <utility>

NES::NoOpSource::NoOpSource(SchemaPtr schema,
                            Runtime::BufferManagerPtr bufferManager,
                            Runtime::QueryManagerPtr queryManager,
                            OperatorId operatorId,
                            OriginId originId,
                            StatisticId statisticId,
                            size_t numSourceLocalBuffers,
                            GatheringMode gatheringMode,
                            const std::string& physicalSourceName)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName) {}
std::optional<NES::Runtime::TupleBuffer> NES::NoOpSource::receiveData() { NES_NOT_IMPLEMENTED(); }
std::string NES::NoOpSource::toString() const { return fmt::format("NoOpSource"); }
NES::SourceType NES::NoOpSource::getType() const { return NES::SourceType::NOOP_SOURCE; }
