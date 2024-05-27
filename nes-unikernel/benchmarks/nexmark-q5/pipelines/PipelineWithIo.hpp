#ifndef CRAZY_RARE_UNUSED_HEADER_GUARD
#define CRAZY_RARE_UNUSED_HEADER_GUARD
#include <DataTypes/PhysicalDataTypes.hpp>
#include <Network/NetworkSink.hpp>
#include <SchemaBuffer.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <UnikernelExecutionPlan.hpp>
#include <UnikernelSink.hpp>
#include <UnikernelSource.hpp>
#include <UnikernelStage.hpp>

#include "../../TestSource.hpp"

namespace NES::Unikernel {
struct CTConfiguration {
    constexpr static SharedQueryId QueryId = NES::SharedQueryId(1);
    constexpr static WorkerId NodeId = NES::WorkerId(2);
    constexpr static const char* NodeIP = "127.0.0.1";
    constexpr static size_t NodePort = 8080;
};
struct SourceConfig35 {
    using SourceType = TCPSource<SourceConfig35>;
    using Schema = Schema<Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<FLOAT64>>;
    constexpr static SharedQueryId QueryId = NES::SharedQueryId(0);
    constexpr static OperatorId UpstreamNodeId = NES::OperatorId(35);
    constexpr static OperatorId OperatorId = NES::OperatorId(35);
    constexpr static OriginId OriginId = NES::OriginId(6);
    constexpr static std::chrono::milliseconds BufferFlushInterval = 100ms;
    constexpr static size_t LocalBuffers = 100;
    constexpr static const char* NodeIP = "127.0.0.1";
    constexpr static FormatTypes Format = NES::FormatTypes::NES_FORMAT;
    constexpr static size_t Port = 8091;
};
struct SourceConfig27 {
    using SourceType = TCPSource<SourceConfig27>;
    using Schema = Schema<Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<FLOAT64>>;
    constexpr static SharedQueryId QueryId = NES::SharedQueryId(0);
    constexpr static OperatorId UpstreamNodeId = NES::OperatorId(27);
    constexpr static OperatorId OperatorId = NES::OperatorId(27);
    constexpr static OriginId OriginId = NES::OriginId(4);
    constexpr static std::chrono::milliseconds BufferFlushInterval = 100ms;
    constexpr static size_t LocalBuffers = 100;
    constexpr static const char* NodeIP = "127.0.0.1";
    constexpr static FormatTypes Format = NES::FormatTypes::NES_FORMAT;
    constexpr static size_t Port = 8091;
};

struct SinkConfig3 {
    constexpr static SharedQueryId QueryId = SharedQueryId(1);
    using SinkType = TestSinkImpl;
    constexpr static size_t OutputSchemaSizeInBytes = 40;
};

using sources = std::tuple<SourceConfig27, SourceConfig35>;
using SubQueryPlan3 = SubQuery<UnikernelSink<SinkConfig3>,
                               PipelineJoin<Stage<2>,
                                            Pipeline<Stage<3>, Stage<4>, Stage<5>, UnikernelSource<SourceConfig27>>,
                                            Pipeline<Stage<7>, Stage<8>, Stage<9>, Stage<10>, UnikernelSource<SourceConfig35>>>>;
using QueryPlan = UnikernelExecutionPlan<SubQueryPlan3>;
}// namespace NES::Unikernel
#endif//CRAZY_RARE_UNUSED_HEADER_GUARD
