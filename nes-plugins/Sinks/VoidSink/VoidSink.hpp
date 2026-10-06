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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that simply dumps input tuples into the void
/// As such the output written to file will always be
/// an empty line
class VoidSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Void";
    explicit VoidSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    TupleBuffer emit(PipelineExecutionContext& pipelineExecutionContext) override;
    void absorb(const TupleBuffer& state, PipelineExecutionContext& pipelineExecutionContext) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct Metrics
    {
        uint64_t receivedTuples;
        uint64_t lastExportedCount;
        uint64_t lastImportedCount;
        uint64_t stoppedCount;
        uint64_t exports;
        uint64_t imports;
    };

    [[nodiscard]] static Metrics getMetrics();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "VoidSink"; }

private:
    uint64_t receivedTuples = 0;
    static std::atomic_uint64_t totalReceivedTuples;
    static std::atomic_uint64_t lastExportedCount;
    static std::atomic_uint64_t lastImportedCount;
    static std::atomic_uint64_t stoppedCount;
    static std::atomic_uint64_t exports;
    static std::atomic_uint64_t imports;
};

struct ConfigParametersVoid
{
    /// Native buffers retain their tuple count; CSV output formatting changes that count to bytes.
    /// Void also accepts file_path for systest's --workingDir flow.
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format", "NATIVE", [](const std::unordered_map<std::string, std::string>&) { return std::optional("NATIVE"); }};

    /// Optional (default empty): Void ignores the path entirely but must not *require* it, so that sinks
    /// configured without a file_path (e.g. DistributedPlanningTest's empty config) still validate.
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        "",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH, OUTPUT_FORMAT);
};
}

FMT_OSTREAM(NES::VoidSink);
