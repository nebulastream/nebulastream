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

#include <cstdint>
#include <ostream>
#include <string_view>

#include <folly/Synchronized.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink-defined config struct: instantiated from the generic config by the SinkConfig registry
/// entry, carried through the SinkDescriptor as std::any, and serialized via reflection of
/// exactly this struct (all members are reflectable).
struct PrintSinkConfig
{
    /// Milliseconds to sleep after each buffer, to simulate a slow consumer.
    uint32_t ingestion;

    static std::expected<PrintSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class PrintSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Print";

    explicit PrintSink(BackpressureController backpressureController, const PrintSinkConfig& config);
    ~PrintSink() override = default;

    PrintSink(const PrintSink&) = delete;
    PrintSink& operator=(const PrintSink&) = delete;
    PrintSink(PrintSink&&) = delete;
    PrintSink& operator=(PrintSink&&) = delete;
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    folly::Synchronized<std::ostream*> outputStream;
    uint32_t ingestion = 0;
};

}
