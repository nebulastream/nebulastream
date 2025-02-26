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

#include <Async/Sources/TCPSource.hpp>

#include <cstring>
#include <format>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/this_coro.hpp>

#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include "Util/Logger/Logger.hpp"

namespace NES::Sources
{

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
{
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    return str;
}

asio::awaitable<void, Executor> TCPSource::open()
try
{
    NES_TRACE("Opening connection to {}:{}", socketHost, socketPort);
    const auto executor = co_await asio::this_coro::executor;
    socket.emplace(executor);
    const auto endpoints = co_await tcp::resolver{executor}.async_resolve(socketHost, socketPort, asio::deferred);
    co_await async_connect(socket.value(), endpoints, asio::deferred);
    NES_DEBUG("Connected to {}:{}.", socketHost, socketPort);
}
catch (boost::system::system_error& error)
{
    throw CannotOpenSource("Failed to connect to {}:{} with error ", socketHost, socketPort, error.what());
}

asio::awaitable<AsyncSource::InternalSourceResult, Executor> TCPSource::fillBuffer(IOBuffer& buffer)
{
    /// Exit early when stop has been requested from the outside.
    if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
    {
        co_return Cancelled{};
    }

    auto [errorCode, bytesRead]
        = co_await async_read(socket.value(), asio::mutable_buffer(buffer.getBuffer(), buffer.getBufferSize()), as_tuple(asio::deferred));

    if (not errorCode)
    {
        co_return Continue{.bytesRead = bytesRead};
    }
    if (errorCode == asio::error::eof)
    {
        co_return EndOfStream{.bytesRead = bytesRead};
    }
    if (errorCode == asio::error::operation_aborted)
    {
        co_return Cancelled{};
    }
    co_return Error{
        .exception = DataIngestionFailure(
            std::format("Failed to read from socket: {}:{} with error: {}", socketHost, socketPort, errorCode.message()))};
}

void TCPSource::close()
try
{
    socket->shutdown(tcp::socket::shutdown_both);
    socket->close();
    NES_DEBUG("Closed TCP socket.");
}
catch (boost::system::system_error& err)
{
    NES_ERROR("Failed to close socket: {}", err.what());
}

Configurations::DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterTCPSourceValidation(SourceValidationRegistryArguments arguments)
{
    return TCPSource::validateAndFormat(arguments.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments arguments)
{
    return std::make_unique<TCPSource>(arguments.sourceDescriptor);
}

}
