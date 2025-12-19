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

#include <RestApiSource.hpp>

#include <cerrno>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>

#include <Configurations/Descriptor.hpp>
#include <cpptrace/from_current.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

RestApiSource::RestApiSource(const SourceDescriptor& sd)
    : endpoint(sd.getFromConfig(ConfigParametersHTTP::ENDPOINT))
    , parsed(parseHttpUrlOrThrow(endpoint))
    , interval(std::chrono::milliseconds(sd.getFromConfig(ConfigParametersHTTP::INTERVAL_MS)))
    , lastPoll(std::chrono::steady_clock::now() - interval) // poll immediately
    , connectTimeoutSeconds(sd.getFromConfig(ConfigParametersHTTP::CONNECT_TIMEOUT))
    , appendNewline(sd.getFromConfig(ConfigParametersHTTP::APPEND_NEWLINE))
{
    NES_TRACE("Init RestApiSource.");
}

std::ostream& RestApiSource::toString(std::ostream& str) const
{
    str << "\nRestApiSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  endpoint: " << endpoint;
    str << "\n  host: " << parsed.host;
    str << "\n  port: " << parsed.port;
    str << "\n  path: " << parsed.path;
    str << "\n  interval_ms: " << interval.count();
    str << "\n  connect_timeout_seconds: " << connectTimeoutSeconds;
    str << "\n  append_newline: " << (appendNewline ? "true" : "false");
    str << ")\n";
    return str;
}

DescriptorConfig::Config RestApiSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersHTTP>(std::move(config), name());
}

void RestApiSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_TRACE("RestApiSource::open()");
    // Nothing persistent to open. Each poll does its own connection.
}

void RestApiSource::close()
{
    NES_TRACE("RestApiSource::close()");
    // Nothing persistent to close.
}

bool RestApiSource::shouldPoll() const
{
    return (std::chrono::steady_clock::now() - lastPoll) >= interval;
}

Source::FillTupleBufferResult RestApiSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    try
    {
        if (stopToken.stop_requested())
        {
            return FillTupleBufferResult::eos();
        }

        if (!shouldPoll())
        {
            // Not end-of-stream; just nothing to emit this cycle.
            return FillTupleBufferResult::withBytes(0);
        }

        lastPoll = std::chrono::steady_clock::now();

        const auto bytesWritten = fetchOnceAndWriteBody(tupleBuffer);

        ++generatedBuffers;
        if (bytesWritten > 0)
        {
            ++generatedTuples; // "one response == one tuple/message" for now
        }

        return FillTupleBufferResult::withBytes(bytesWritten);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("RestApiSource failed to fill TupleBuffer. Error: {}.", e.what());
        throw;
    }
}

size_t RestApiSource::fetchOnceAndWriteBody(TupleBuffer& tupleBuffer)
{
    std::string body = httpGetBody(parsed);

    if (appendNewline)
    {
        body.push_back('\n');
    }

    const auto& mem = tupleBuffer.getAvailableMemoryArea();
    if (body.size() > mem.size())
    {
        NES_ERROR("RestApiSource response body too large ({} bytes) for TupleBuffer ({} bytes).", body.size(), mem.size());
        return 0;
    }

    std::memcpy(mem.data(), body.data(), body.size());
    return body.size();
}

RestApiSource::ParsedUrl RestApiSource::parseHttpUrlOrThrow(const std::string& url)
{
    // Minimal parser for: http://host[:port]/path
    // No TLS support in this basic version.
    const std::string prefix = "http://";
    if (url.rfind(prefix, 0) != 0)
    {
        throw InvalidConfigParameter("RestApiSource endpoint must start with 'http://'. Got: {}", url);
    }

    std::string rest = url.substr(prefix.size()); // host[:port]/path...
    std::string hostPort;
    std::string path = "/";

    const auto slashPos = rest.find('/');
    if (slashPos == std::string::npos)
    {
        hostPort = rest;
    }
    else
    {
        hostPort = rest.substr(0, slashPos);
        path = rest.substr(slashPos);
        if (path.empty())
        {
            path = "/";
        }
    }

    std::string host;
    std::string port = "80";

    const auto colonPos = hostPort.find(':');
    if (colonPos == std::string::npos)
    {
        host = hostPort;
    }
    else
    {
        host = hostPort.substr(0, colonPos);
        port = hostPort.substr(colonPos + 1);
        if (port.empty())
        {
            port = "80";
        }
    }

    if (host.empty())
    {
        throw InvalidConfigParameter("RestApiSource endpoint has empty host. Got: {}", url);
    }

    return ParsedUrl{.host = std::move(host), .port = std::move(port), .path = std::move(path)};
}

std::string RestApiSource::httpGetBody(const ParsedUrl& u)
{
    // Resolve host
    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    const int errorCode = getaddrinfo(u.host.c_str(), u.port.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("RestApiSource getaddrinfo failed: {}", gai_strerror(errorCode));
    }

    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    int sockfd = -1;
    const addrinfo* rp = result;

    while (rp != nullptr)
    {
        sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sockfd != -1)
        {
            // Set timeouts on socket
            timeval timeout{};
            timeout.tv_sec = connectTimeoutSeconds;
            timeout.tv_usec = 1; // avoid "never timeout" corner case
            setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

            if (connect(sockfd, rp->ai_addr, rp->ai_addrlen) == 0)
            {
                break; // connected
            }

            ::close(sockfd);
            sockfd = -1;
        }
        rp = rp->ai_next;
    }

    if (sockfd == -1)
    {
        const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
        throw CannotOpenSource("RestApiSource could not connect to {}:{} ({})", u.host, u.port, strerrorResult);
    }

    // Build request
    std::string request;
    request.reserve(256 + u.path.size() + u.host.size());
    request += "GET ";
    request += u.path;
    request += " HTTP/1.1\r\n";
    request += "Host: ";
    request += u.host;
    request += "\r\n";
    request += "Connection: close\r\n";
    request += "Accept: */*\r\n";
    request += "\r\n";

    // Send request
    const char* data = request.c_str();
    size_t remaining = request.size();
    while (remaining > 0)
    {
        const ssize_t sent = ::send(sockfd, data, remaining, 0);
        if (sent <= 0)
        {
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            ::close(sockfd);
            throw CannotOpenSource("RestApiSource send failed: {}", strerrorResult);
        }
        data += sent;
        remaining -= static_cast<size_t>(sent);
    }

    // Read response
    std::string response;
    response.reserve(4096);

    std::array<char, 4096> buf{};
    while (true)
    {
        const ssize_t n = ::recv(sockfd, buf.data(), buf.size(), 0);
        if (n > 0)
        {
            response.append(buf.data(), static_cast<size_t>(n));
            continue;
        }
        if (n == 0)
        {
            break; // connection closed by server
        }
        // n < 0
        const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
        ::close(sockfd);
        throw CannotOpenSource("RestApiSource recv failed: {}", strerrorResult);
    }

    ::close(sockfd);

    // Split headers / body
    const std::string sep = "\r\n\r\n";
    const auto pos = response.find(sep);
    if (pos == std::string::npos)
    {
        // No headers found; return whole response as body
        return response;
    }

    const std::string headers = response.substr(0, pos);
    const std::string body = response.substr(pos + sep.size());

    // Very light status check: look for "HTTP/1.1 200"
    if (headers.rfind("HTTP/1.1 200", 0) != 0 && headers.rfind("HTTP/1.0 200", 0) != 0)
    {
        NES_WARNING("RestApiSource received non-200 response. Headers: {}", headers);
        // Still return body; caller can decide what to do.
    }

    return body;
}

SourceValidationRegistryReturnType RegisterRestApiSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return RestApiSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterRestApiSource(SourceRegistryArguments args)
{
    return std::make_unique<RestApiSource>(args.sourceDescriptor);
}


} // namespace NES
