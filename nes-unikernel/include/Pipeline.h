#ifndef NES_UNIKERNEL_PIPELINE_H
#define NES_UNIKERNEL_PIPELINE_H
#include <UnikernelExecutionPlan.hpp>
#include <UnikernelSink.hpp>
#include <UnikernelSource.hpp>
#include <UnikernelStage.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Network/NetworkSink.hpp>
#include <DataTypes/PhysicalDataTypes.hpp>
#include <SchemaBuffer.hpp>

namespace NES::Unikernel {
struct CTConfiguration {
    constexpr static unsigned long QueryID = 0;
    constexpr static unsigned long NodeID = 2;
    constexpr static const char* NodeIP = "127.0.0.1";
    constexpr static unsigned long NodePort = 8080;
};
struct KafkaSinkConfig15 {
    constexpr static unsigned long QueryID = 0;
    using KafkaSchema = Schema<Field<INT8>, Field<FLOAT32>>;
    using SinkType = typename NES::KafkaSink<KafkaSchema>;
    constexpr static unsigned long QuerySubplanID = 8;
    constexpr static const char* Broker = "Broker";
    constexpr static const char* Topic = "Topic";
};

struct SinkConfig15 {
    constexpr static unsigned long QueryID = 0;
    using SinkType = typename NES::Network::NetworkSink;
    constexpr static unsigned long QuerySubplanID = 8;
    constexpr static unsigned long OutputSchemaSizeInBytes = 96;
    constexpr static unsigned long DownstreamNodeID = 1;
    constexpr static unsigned long DownstreamPartitionID = 0;
    constexpr static unsigned long DownstreamSubPartitionID = 0;
    constexpr static const char* DownstreamNodeHostname = "127.0.0.1";
    constexpr static unsigned long DownstreamOperatorID = 15;
    constexpr static unsigned int DownstreamNodePort = 8082;
    constexpr static unsigned long LocalBuffers = 100;
};
using Sink15 = UnikernelSink<KafkaSinkConfig15>;
struct SourceConfig17 {
    using SourceType = TCPSource<SourceConfig17>;
    using Schema = Schema<Field<INT8>, Field<FLOAT32>>;
    constexpr static OriginId OriginId = 1;
    constexpr static unsigned long UpstreamNodeID = 3;
    constexpr static OperatorId OperatorId = 1;
    constexpr static unsigned long QueryID = 0;
    constexpr static unsigned long LocalBuffers = 100;
};

using Source17 = UnikernelSource<SourceConfig17>;
struct SourceConfig13 {
    using SourceType = NES::Network::NetworkSource;
    constexpr static unsigned long QueryID = 0;
    constexpr static unsigned long UpstreamNodeID = 4;
    constexpr static unsigned long UpstreamPartitionID = 0;
    constexpr static unsigned long UpstreamSubPartitionID = 0;
    constexpr static const char* UpstreamNodeHostname = "127.0.0.1";
    constexpr static unsigned long UpstreamOperatorID = 13;
    constexpr static unsigned int UpstreamNodePort = 8081;
    constexpr static unsigned long LocalBuffers = 100;
};
using Source13 = UnikernelSource<SourceConfig13>;
struct SourceConfig19 {
    using SourceType = NES::Network::NetworkSource;
    constexpr static unsigned long QueryID = 0;
    constexpr static unsigned long UpstreamNodeID = 5;
    constexpr static unsigned long UpstreamPartitionID = 0;
    constexpr static unsigned long UpstreamSubPartitionID = 0;
    constexpr static const char* UpstreamNodeHostname = "127.0.0.1";
    constexpr static unsigned long UpstreamOperatorID = 19;
    constexpr static unsigned int UpstreamNodePort = 8084;
    constexpr static unsigned long LocalBuffers = 100;
};
using Source19 = UnikernelSource<SourceConfig19>;
using SubQuery8 = SubQuery<Sink15,
                           PipelineJoin<Stage<2>,
                                        PipelineJoin<Stage<3>, Pipeline<Stage<4>, Source17>, Pipeline<Stage<6>, Source13>>,
                                        Pipeline<Stage<8>, Source19>>>;
using QueryPlan = UnikernelExecutionPlan<SubQuery8>;
}// namespace NES::Unikernel
#endif//NES_UNIKERNEL_PIPELINE_H
