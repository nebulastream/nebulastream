#ifndef CRAZY_RARE_UNUSED_HEADER_GUARD
#define CRAZY_RARE_UNUSED_HEADER_GUARD

#include <UnikernelExecutionPlan.hpp>
#include <UnikernelSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Network/NetworkSink.hpp>
#include <UnikernelSource.hpp>
#include <UnikernelStage.hpp>
#include <SchemaBuffer.hpp>
#include <DataTypes/PhysicalDataTypes.hpp>

namespace NES::Unikernel {
    struct CTConfiguration {
        constexpr static size_t QueryID = 0;
        constexpr static size_t NodeID = 2;
        constexpr static const char *NodeIP = "127.0.0.1";
        constexpr static size_t NodePort = 8080;
    };
    struct SourceConfig17 {
        using SourceType = Network::NetworkSource;
        constexpr static size_t QueryID = 0;
        constexpr static size_t UpstreamNodeID = 4;
        constexpr static size_t UpstreamPartitionID = 0;
        constexpr static size_t UpstreamSubPartitionID = 0;
        constexpr static const char *UpstreamNodeHostname = "127.0.0.1";
        constexpr static size_t UpstreamOperatorID = 17;
        constexpr static size_t UpstreamNodePort = 8084;
        constexpr static size_t LocalBuffers = 100;
    };
    struct SourceConfig13 {
        using SourceType = Network::NetworkSource;
        constexpr static size_t QueryID = 0;
        constexpr static size_t UpstreamNodeID = 3;
        constexpr static size_t UpstreamPartitionID = 0;
        constexpr static size_t UpstreamSubPartitionID = 0;
        constexpr static const char *UpstreamNodeHostname = "127.0.0.1";
        constexpr static size_t UpstreamOperatorID = 13;
        constexpr static size_t UpstreamNodePort = 8081;
        constexpr static size_t LocalBuffers = 100;
    };
    struct SourceConfig29 {
        using SourceType = TCPSource<SourceConfig29>;
        using Schema = Schema<Field<INT64>, Field<INT64>>;
        constexpr static size_t QueryID = 0;
        constexpr static size_t UpstreamNodeID = 29;
        constexpr static size_t OperatorId = 29;
        constexpr static size_t OriginId = 4;
        constexpr static std::chrono::milliseconds BufferFlushInterval = 100ms;
        constexpr static size_t LocalBuffers = 100;
        constexpr static const char *NodeIP = "127.0.0.1";
        constexpr static short Port = 8092;
    };
    struct SinkConfig7 {
        constexpr static size_t QueryID = 0;
        using SinkType = typename NES::Network::NetworkSink;
        constexpr static size_t QuerySubplanID = 7;
        constexpr static size_t OutputSchemaSizeInBytes = 96;
        constexpr static size_t DownstreamNodeID = 1;
        constexpr static size_t DownstreamPartitionID = 0;
        constexpr static size_t DownstreamSubPartitionID = 0;
        constexpr static const char *DownstreamNodeHostname = "127.0.0.1";
        constexpr static size_t DownstreamOperatorID = 15;
        constexpr static size_t DownstreamNodePort = 8085;
        constexpr static size_t LocalBuffers = 100;
    };
    using SubQueryPlan7 = SubQuery<UnikernelSink<SinkConfig7>, PipelineJoin<Stage<4>, PipelineJoin<Stage<5>, Pipeline<Stage<6>, UnikernelSource<SourceConfig29>>, Pipeline<Stage<8>, UnikernelSource<SourceConfig13>>>, Pipeline<Stage<10>, UnikernelSource<SourceConfig17>>>>;
    using QueryPlan = UnikernelExecutionPlan<SubQueryPlan7>;
}
#endif //CRAZY_RARE_UNUSED_HEADER_GUARD

