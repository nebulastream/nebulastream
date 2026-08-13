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

#include <ostream>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink-defined config struct: the void sink declares no config parameters, but the (empty)
/// struct still travels through the SinkDescriptor and its registry entry so that descriptors
/// are handled uniformly.
struct VoidSinkConfig
{
    static std::expected<VoidSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// A sink that simply dumps input tuples into the void
/// As such the output written to file will always be
/// an empty line
class VoidSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Void";
    explicit VoidSink(BackpressureController backpressureController, const VoidSinkConfig& config);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "VoidSink"; }
};
}

FMT_OSTREAM(NES::VoidSink);
