/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>
#include <StatisticService.grpc.pb.h>

namespace NES
{

struct ConfigParametersGrpcSink
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "GRPC_HOST",
        "localhost",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "GRPC_PORT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

    /// OUTPUT_FORMAT is inherited from SinkDescriptor::parameterMap rather than redeclared with a default:
    /// a redeclaration here is shadowed by the base one and validation still reports it as unspecified.
    /// Callers must set it to CSV, which is what this sink parses (see the class comment).
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, HOST, PORT);
};

/// Reports each incoming tuple to a StatisticInterfaceService as a StatisticReport.
///
/// Reads the CSV rows the formatter produces and locates its four columns by resolving their names against the
/// descriptor's ordered schema once, in the constructor. That is the same schema FileSink uses to write its
/// header, so it is exactly the order the rows arrive in.
///
/// Two things this deliberately does not do. It does not compute raw memory offsets the way the branch this is
/// ported from did: the raw layout follows the physical schema, which a sink never sees. And it does not parse a
/// header out of the stream: the header is not in the stream at all, because SchemaFormatter notes that it
/// "cannot be moved into the emit phase" and each sink writes it separately. Resolving up front also keeps
/// execute() free of shared mutable state, which matters because rows arrive on several worker threads at once.
class GrpcSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Grpc";

    explicit GrpcSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "GrpcSink(" << grpcHost << ":" << grpcPort << ")"; }

private:
    /// Reports one formatted data row. Const: nothing here mutates the sink, so concurrent rows are safe.
    void reportRow(std::string_view line) const;

    std::string grpcHost;
    uint32_t grpcPort;

    /// Column positions resolved from the descriptor's ordered schema. Absent when the incoming schema does not
    /// carry that field, in which case the corresponding proto field is left at its default.
    std::optional<size_t> statisticIdColumn;
    std::optional<size_t> startTsColumn;
    std::optional<size_t> endTsColumn;
    std::optional<size_t> valueColumn;
    size_t columnCount{0};

    std::unique_ptr<StatisticInterfaceService::Stub> stub;
};

}

FMT_OSTREAM(NES::GrpcSink);
