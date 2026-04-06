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

#include <UnixSocketSink.hpp>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

UnixSocketSink::UnixSocketSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , socketPath(sinkDescriptor.getFromConfig(ConfigParametersUnixSocket::SOCKET_PATH))
{
}

UnixSocketSink::~UnixSocketSink()
{
    if (serverFd >= 0)
    {
        close(serverFd);
        unlink(socketPath.c_str());
    }
}

std::ostream& UnixSocketSink::toString(std::ostream& os) const
{
    os << "UnixSocketSink(socketPath: " << socketPath << ")";
    return os;
}

void UnixSocketSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up unix socket sink: {}", *this);

    // Remove stale socket file if it exists
    unlink(socketPath.c_str());

    serverFd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (serverFd < 0)
    {
        throw CannotOpenSink("Could not create unix socket: {}", std::strerror(errno));
    }
    fcntl(serverFd, F_SETFL, fcntl(serverFd, F_GETFL) | O_NONBLOCK);

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (socketPath.size() >= sizeof(addr.sun_path))
    {
        close(serverFd);
        serverFd = -1;
        throw CannotOpenSink("Socket path too long (max {}): {}", sizeof(addr.sun_path) - 1, socketPath);
    }
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(serverFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        close(serverFd);
        serverFd = -1;
        throw CannotOpenSink("Could not bind unix socket to {}: {}", socketPath, std::strerror(errno));
    }

    if (listen(serverFd, 5) < 0)
    {
        close(serverFd);
        serverFd = -1;
        unlink(socketPath.c_str());
        throw CannotOpenSink("Could not listen on unix socket {}: {}", socketPath, std::strerror(errno));
    }

    NES_INFO("Unix socket sink listening on {}", socketPath);
}

void UnixSocketSink::acceptPendingConnections()
{
    while (true)
    {
        const int clientFd = accept(serverFd, nullptr, nullptr);
        if (clientFd < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break; // No more pending connections
            }
            NES_WARNING("Failed to accept connection on unix socket {}: {}", socketPath, std::strerror(errno));
            break;
        }
        fcntl(clientFd, F_SETFL, fcntl(clientFd, F_GETFL) | O_NONBLOCK);

        NES_DEBUG("New client connected to unix socket sink {}, fd={}", socketPath, clientFd);
        clientFds.wlock()->push_back(clientFd);
    }
}

void UnixSocketSink::sendToClients(const char* data, size_t len)
{
    auto locked = clientFds.wlock();
    auto it = locked->begin();
    while (it != locked->end())
    {
        const ssize_t written = send(*it, data, len, MSG_NOSIGNAL);
        if (written < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // Client is slow — drop this buffer for this client
                NES_DEBUG("Unix socket client fd={} is slow, dropping buffer", *it);
                ++it;
                continue;
            }
            // Client disconnected or error — remove it
            NES_DEBUG("Unix socket client fd={} disconnected: {}", *it, std::strerror(errno));
            close(*it);
            it = locked->erase(it);
            continue;
        }
        ++it;
    }
}

void UnixSocketSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in UnixSocketSink.");

    // Accept any new connections before writing
    acceptPendingConnections();

    // If nobody is listening, skip the work
    if (clientFds.rlock()->empty())
    {
        return;
    }

    BufferIterator iterator{inputTupleBuffer};
    std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
    while (element.has_value())
    {
        sendToClients(element.value().buffer.getAvailableMemoryArea<char>().data(),
                      element.value().contentLength);
        element = iterator.getNextElement();
    }
}

void UnixSocketSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing unix socket sink: {}", socketPath);

    // Close all client connections
    {
        auto locked = clientFds.wlock();
        for (const int fd : *locked)
        {
            close(fd);
        }
        locked->clear();
    }

    // Close server socket and remove socket file
    if (serverFd >= 0)
    {
        close(serverFd);
        serverFd = -1;
    }
    unlink(socketPath.c_str());
}

DescriptorConfig::Config UnixSocketSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersUnixSocket>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterUnixSocketSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return UnixSocketSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterUnixSocketSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<UnixSocketSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
