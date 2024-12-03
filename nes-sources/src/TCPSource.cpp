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

#include <cstring>
#include <ostream>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <TCPSource.hpp>
#include "ErrorHandling.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>


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

asio::awaitable<Source::InternalSourceResult> TCPSource::fillBuffer(ByteBuffer& buffer)
{
    auto [errorCode, bytesRead] = co_await asio::async_read(
        socket.value(), asio::mutable_buffer(buffer.getBuffer(), buffer.getBufferSize()), asio::as_tuple(asio::use_awaitable));

    if (errorCode)
    {
        if (errorCode == asio::error::eof)
        {
            co_return EoS{.dataAvailable = bytesRead != 0};
        }
        co_return Error{boost::system::system_error{errorCode}};
    }
    co_return Continue{};
}

asio::awaitable<void> TCPSource::close(asio::io_context& /*ioc*/)
{
    socket->close();
    co_return;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
TCPSource::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceValidationGeneratedRegistrar::RegisterSourceValidationTCP(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return TCPSource::validateAndFormat(std::move(sourceConfig));
}

std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterTCPSource(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<TCPSource>(sourceDescriptor);
}

}
