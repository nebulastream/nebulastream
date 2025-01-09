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

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/TCPSink.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <arpa/inet.h>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <netinet/in.h>
#include <sys/socket.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

TCPSink::TCPSink(const SinkDescriptor& sinkDescriptor)
    : Sink(), host(sinkDescriptor.getFromConfig(ConfigParametersTCP::HOST)), port(sinkDescriptor.getFromConfig(ConfigParametersTCP::PORT))
{
}

std::ostream& TCPSink::toString(std::ostream& str) const
{
    str << fmt::format("TCPSink(host: {}, port: {})", host, port);
    return str;
}

void TCPSink::start(Runtime::Execution::PipelineExecutionContext&)
{
    int sock = 0;
    sockaddr_in serv_addr;

    if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        throw CannotOpenSink("Socket creation error: {}", strerror(errno));
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0)
    {
        throw CannotOpenSink("Invalid address: {}", strerror(errno));
    }

    if (connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
        throw CannotOpenSink("Connection failed: {}", strerror(errno));
    }

    socket = sock;
}
void TCPSink::execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in TCPSink.");
    /// Hack: currently this only sends the first childbuffer (which contains the image)
    auto buffer = inputTupleBuffer.loadChildBuffer(0);
    auto size = *buffer.getBuffer<uint32_t>();
    auto bytesWritten = write(*socket, buffer.getBuffer<uint8_t>() + sizeof(uint32_t), size);
    if (bytesWritten < 0)
    {
        throw CannotOpenSink("Connection failed: {}", strerror(errno));
    }
    if (bytesWritten == 0)
    {
        throw CannotOpenSink("Connection Terminated");
    }
    if (bytesWritten < size)
    {
        throw CannotOpenSink("Sent a partial frame, this screws up the VideoStream. Terminating query");
    }
}

void TCPSink::stop(Runtime::Execution::PipelineExecutionContext&)
{
    NES_DEBUG("Closing TCP sink, host={}, port={}", host, port);
    if (socket)
    {
        ::close(*socket);
    }
}

Configurations::DescriptorConfig::Config TCPSink::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterTCPSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return TCPSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterTCPSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<TCPSink>(sinkRegistryArguments.sinkDescriptor);
}

}
