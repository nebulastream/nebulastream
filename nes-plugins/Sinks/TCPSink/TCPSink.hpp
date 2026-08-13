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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <TCPChannel.hpp>

namespace NES
{

struct TCPSinkConfig
{
    std::string socketHost;
    uint32_t socketPort{};
    uint32_t connectTimeoutMs{};
    uint32_t closeTimeoutMs{};
    size_t maxQueuedBuffers{};

    static std::expected<TCPSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Streams the bytes produced by the configured output formatter to a TCP server.
/// Delivery is best-effort across reconnects: data sent before a disconnect may be lost, and the buffer being written
/// is sent again from its start on the new connection.
class TCPSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "TCP";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    explicit TCPSink(BackpressureController backpressureController, const TCPSinkConfig& config, const SinkDescriptor& sinkDescriptor);
    ~TCPSink() override = default;

    TCPSink(const TCPSink&) = delete;
    TCPSink& operator=(const TCPSink&) = delete;
    TCPSink(TCPSink&&) = delete;
    TCPSink& operator=(TCPSink&&) = delete;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    TCPChannel channel;
    BackpressureHandler backpressureHandler;
};

}

FMT_OSTREAM(NES::TCPSink);
