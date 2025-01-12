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

#include "TCPSource.hpp"

#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
{
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << ")\n";
    return str;
}

asio::awaitable<void> TCPSource::open(asio::io_context& ioc)
{
    asio::ip::tcp::resolver tcpResolver{ioc};

    auto [errorCode, endpoints] = co_await tcpResolver.async_resolve(socketHost, socketPort, asio::as_tuple(asio::use_awaitable));
    if (errorCode)
    {
        throw CannotOpenSource("TCPSource: Failed to resolve host {} on port {}", socketHost, socketPort);
    }

    socket.emplace(ioc);
    co_await async_connect(socket.value(), endpoints, asio::use_awaitable);
}

asio::awaitable<Source::InternalSourceResult> TCPSource::fillBuffer(IOBuffer& buffer)
{
    auto [errorCode, bytesRead] = co_await asio::async_read(
        socket.value(), asio::mutable_buffer(buffer.getBuffer(), buffer.getBufferSize()), asio::as_tuple(asio::use_awaitable));

    if (errorCode)
    {
        if (errorCode == asio::error::eof)
        {
            co_return EndOfStream{.dataAvailable = bytesRead != 0};
        }
        if (errorCode == asio::error::operation_aborted)
        {
            co_return Cancelled{};
        }
        co_return Error{boost::system::system_error{errorCode}};
    }
    co_return Continue{};
}

void TCPSource::close()
{
    socket->close();
}

void TCPSource::cancel()
{
    socket->cancel();
}


std::unique_ptr<Configurations::DescriptorConfig::Config> TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

std::unique_ptr<SourceValidationRegistryReturnType>
SourceValidationGeneratedRegistrar::RegisterTCPSourceValidation(const SourceValidationRegistryArguments& arguments)
{
    return TCPSource::validateAndFormat(arguments.config);
}

std::unique_ptr<SourceRegistryReturnType> SourceGeneratedRegistrar::RegisterTCPSource(const SourceRegistryArguments& arguments)
{
    return std::make_unique<TCPSource>(arguments.sourceDescriptor);
}

}
