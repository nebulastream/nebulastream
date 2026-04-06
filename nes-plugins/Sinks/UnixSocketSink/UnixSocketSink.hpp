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
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that serves formatted TupleBuffers over a Unix domain socket.
/// NES acts as the server: it binds and listens on the socket path.
/// Clients (e.g., system test tools) connect to read the data stream.
/// Writes are non-blocking and best-effort — if no client is connected
/// or a client is slow, data is silently dropped.
class UnixSocketSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "UnixSocket";
    explicit UnixSocketSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~UnixSocketSink() override;

    UnixSocketSink(const UnixSocketSink&) = delete;
    UnixSocketSink& operator=(const UnixSocketSink&) = delete;
    UnixSocketSink(UnixSocketSink&&) = delete;
    UnixSocketSink& operator=(UnixSocketSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Accept any pending connections on the server socket (non-blocking).
    void acceptPendingConnections();

    /// Send data to all connected clients. Removes clients on error.
    void sendToClients(const char* data, size_t len);

    std::string socketPath;
    int serverFd = -1;
    folly::Synchronized<std::vector<int>> clientFds;
};

struct ConfigParametersUnixSocket
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SOCKET_PATH{
        "socket_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SOCKET_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, SOCKET_PATH);
};

}

FMT_OSTREAM(NES::UnixSocketSink);
