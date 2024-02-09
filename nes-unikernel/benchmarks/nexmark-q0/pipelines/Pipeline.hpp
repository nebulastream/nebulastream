#ifndef CRAZY_RARE_UNUSED_HEADER_GUARD
#define CRAZY_RARE_UNUSED_HEADER_GUARD
#include <DataTypes/PhysicalDataTypes.hpp>
#include <SchemaBuffer.hpp>
#include <UnikernelExecutionPlan.hpp>
#include <UnikernelSink.hpp>
#include <UnikernelSource.hpp>
#include <UnikernelStage.hpp>

#include "../../TestSource.hpp"
namespace NES::Unikernel {
struct CTConfiguration {
    constexpr static size_t QueryId = 0;
    constexpr static size_t NodeId = 2;
    constexpr static const char* NodeIP = "127.0.0.1";
    constexpr static size_t NodePort = 8080;
    constexpr static bool CopyBuffer = false;
};
struct SourceConfig35 {
    using SourceType = TestSourceImpl<SourceConfig35>;
    using Schema = Schema<Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<FLOAT64>>;
    constexpr static size_t QueryId = 0;
    constexpr static size_t UpstreamNodeId = 35;
    constexpr static size_t OperatorId = 35;
    constexpr static size_t OriginId = 6;
    constexpr static size_t LocalBuffers = 100;
    constexpr static bool CopyBuffer = false;
};

struct SinkConfig3 {
    constexpr static size_t QueryId = 0;
    using SinkType = TestSinkImpl;
    constexpr static size_t OutputSchemaSizeInBytes = 40;
};

using SubQueryPlan3 = SubQuery<UnikernelSink<SinkConfig3>, Pipeline<UnikernelSource<SourceConfig35>>>;
using QueryPlan = UnikernelExecutionPlan<SubQueryPlan3>;
}// namespace NES::Unikernel
#endif//CRAZY_RARE_UNUSED_HEADER_GUARD
