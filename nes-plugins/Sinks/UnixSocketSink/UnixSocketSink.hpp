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

#include <expected>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

struct UnixSocketSinkConfig
{
    std::string path;

    static std::expected<UnixSocketSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Streams formatted bytes to an existing Unix-domain stream socket.
/// The peer owns the listening socket and its filesystem path.
class UnixSocketSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "unix_socket";

    explicit UnixSocketSink(
        BackpressureController backpressureController, const UnixSocketSinkConfig& config, const SinkDescriptor& sinkDescriptor);
    ~UnixSocketSink() override = default;

    UnixSocketSink(const UnixSocketSink&) = delete;
    UnixSocketSink& operator=(const UnixSocketSink&) = delete;
    UnixSocketSink(UnixSocketSink&&) = delete;
    UnixSocketSink& operator=(UnixSocketSink&&) = delete;

    void start(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    struct Connection
    {
        explicit Connection(const std::string& path);
        ~Connection();

        Connection(const Connection&) = delete;
        Connection& operator=(const Connection&) = delete;
        Connection(Connection&&) = delete;
        Connection& operator=(Connection&&) = delete;

        int descriptor{-1};
    };

    void write(const Connection& connection, std::string_view contents) const;

    std::string path;
    folly::Synchronized<std::optional<Connection>> connection;
    SchemaFormatter schemaFormatter;
};

}

FMT_OSTREAM(NES::UnixSocketSink);
