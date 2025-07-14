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

#include <AudioSource.hpp>

#include <algorithm>
#include <bit>
#include <cerrno> /// For socket error
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <sys/select.h>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

#include "GeneratorDataRegistry.hpp"

namespace NES::Sources
{

AudioSource::AudioSource(const SourceDescriptor& sourceDescriptor)
    : realTimestamp(sourceDescriptor.getFromConfig(ConfigParametersAudio::REAL_TIMESTAMP))
    , socketHost(sourceDescriptor.getFromConfig(ConfigParametersAudio::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersAudio::PORT)))
    , sampleRate(sourceDescriptor.getFromConfig(ConfigParametersAudio::SAMPLE_RATE))
    , sampleWidth(sourceDescriptor.getFromConfig(ConfigParametersAudio::SAMPLE_WIDTH))
{
    NES_INFO("AudioSource::AudioSource: Init AudioSource.");
}

std::ostream& AudioSource::toString(std::ostream& str) const
{
    str << "\nAudioSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << ")\n";
    return str;
}

bool AudioSource::tryToConnect(const addrinfo* result, const int flags)
{
    const std::chrono::seconds socketConnectDefaultTimeout{1};

    /// we try each addrinfo until we successfully create a socket
    while (result != nullptr)
    {
        sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);

        if (sockfd != -1)
        {
            break;
        }
        result = result->ai_next;
    }

    /// check if we found a vaild address
    if (result == nullptr)
    {
        NES_ERROR("No valid address found to create socket.");
        return false;
    }

    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    /// set timeout for both blocking receive and send calls
    /// if timeout is set to zero, then the operation will never timeout
    /// (https://linux.die.net/man/7/socket)
    /// as a workaround, we implicitly add one microsecond to the timeout
    timeval timeout{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    connection = connect(sockfd, result->ai_addr, result->ai_addrlen);

    /// if the AudioSource did not establish a connection, try with timeout
    if (connection < 0)
    {
        if (errno != EINPROGRESS)
        {
            close();
            /// if connection was unsuccessful, throw an exception with context using errno
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }

        /// Set the timeout for the connect attempt
        fd_set fdset;
        timeval timeValue{socketConnectDefaultTimeout.count(), IMPLICIT_TIMEOUT_USEC};

        FD_ZERO(&fdset);
        FD_SET(sockfd, &fdset);

        connection = select(sockfd + 1, nullptr, &fdset, nullptr, &timeValue);
        if (connection <= 0)
        {
            /// Timeout or error
            errno = ETIMEDOUT;
            close();
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }

        /// Check if connect succeeded
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || (error != 0))
        {
            errno = error;
            close();
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }
    }
    return true;
}

void AudioSource::open()
{
    NES_INFO("AudioSource::open: Trying to create socket and connect. {}:{}", socketHost, socketPort);

    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo() ;

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("Failed getaddrinfo with error: {}", gai_strerror(errorCode));
    }

    /// make sure that result is cleaned up automatically (RAII)
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    const int flags = fcntl(sockfd, F_GETFL, 0);

    try
    {
        tryToConnect(result, flags);
    }
    catch (const std::exception& e)
    {
        ::close(sockfd); /// close socket to clean up state
        throw CannotOpenSource("Could not establish connection! Error: {}", e.what());
    }

    /// Set connection to non-blocking again to enable a timeout in the 'read()' call
    fcntl(sockfd, F_SETFL, flags);

    current = realTimestamp ? std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                            : std::chrono::nanoseconds::zero();

    NES_INFO("AudioSource::open: Connected to server.");
}


size_t fillBuffer(int sockfd, NES::Memory::TupleBuffer& tupleBuffer, size_t numberOfSamples, size_t sampleWidth)
{
    const ssize_t bufferSizeReceived = read(sockfd, tupleBuffer.getBuffer(), numberOfSamples * sampleWidth);
    if (bufferSizeReceived < 0)
    {
        throw CannotOpenSource("Failed to read from socket. Error: {}", strerror(errno));
    }
    return static_cast<size_t>(bufferSizeReceived);
}

size_t AudioSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider&, const std::stop_token&)
{
    static std::chrono::nanoseconds timePerSample{1000000000 / sampleRate};
    struct __attribute__((packed)) Tuple
    {
        uint64_t timestamp;
        float value;
    };

    try
    {
        size_t bufferSize = tupleBuffer.getBufferSize();
        size_t tuplePerBuffer = bufferSize / (4 + 8);
        size_t numberOfBytesReceived = fillBuffer(sockfd, tupleBuffer, tuplePerBuffer, sampleWidth);
        size_t numberOfTuplesReceived = numberOfBytesReceived / sampleWidth;
        /// Fixup the received data by inserting timestamps.
        auto tuples = std::span{tupleBuffer.getBuffer<Tuple>(), numberOfTuplesReceived};

        auto transformation = [&]<typename SampleType>()
        {
            auto values = std::span(tupleBuffer.getBuffer<SampleType>(), numberOfBytesReceived / sizeof(SampleType));
            auto convert = [&, this](const auto& p)
            {
                auto [index, value] = p;
                return Tuple{static_cast<uint64_t>((current + (index * timePerSample)).count()), static_cast<float>(value)};
            };

            std::ranges::copy_backward(views::enumerate(values) | std::views::transform(convert), tuples.end());
            tupleBuffer.setNumberOfTuples(numberOfTuplesReceived);
            current += numberOfTuplesReceived * timePerSample;
        };

        switch (sampleWidth)
        {
            case 1:
                transformation.operator()<int8_t>();
                break;
            case 2:
                transformation.operator()<int16_t>();
                break;
            case 4:
                transformation.operator()<int32_t>();
                break;
            case 8:
                transformation.operator()<int64_t>();
                break;
            default:
                throw CannotOpenSource("Unsupported sample width: {}", sampleWidth);
        }

        return numberOfTuplesReceived;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("AudioSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

void AudioSource::close()
{
    NES_DEBUG("AudioSource::close: trying to close connection.");
    if (connection >= 0)
    {
        ::close(sockfd);
        NES_INFO("AudioSource::close: connection closed.");
    }
}

NES::Configurations::DescriptorConfig::Config AudioSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersAudio>(std::move(config), name());
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterAudioSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return AudioSource::validateAndFormat(std::move(sourceConfig.config));
}

namespace
{
void validate(const SourceRegistryArguments& args)
{
    const auto schema = args.sourceDescriptor.getLogicalSource().getSchema();
    if (schema->getNumberOfFields() != 2)
    {
        throw DifferentFieldTypeExpected("Audio Source expected a schema of (Timestamp: UINT64, value: FLOAT32)");
    }

    const auto timestampField = schema->getFieldAt(0);
    const auto valueField = schema->getFieldAt(1);

    if (!timestampField.dataType.isType(DataType::Type::UINT64))
    {
        throw DifferentFieldTypeExpected("Audio Source expected a schema of (Timestamp: UINT64, value: FLOAT32)");
    }

    if (!valueField.dataType.isType(DataType::Type::FLOAT32))
    {
        throw DifferentFieldTypeExpected("Audio Source expected a schema of (Timestamp: UINT64, value: FLOAT32)");
    }
}
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterAudioSource(SourceRegistryArguments sourceRegistryArguments)
{
    validate(sourceRegistryArguments);
    return std::make_unique<AudioSource>(sourceRegistryArguments.sourceDescriptor);
}


///NOLINTNEXTLINE (performance-unnecessary-value-param)
GeneratorDataRegistryReturnType
GeneratorDataGeneratedRegistrar::RegisterAudioGeneratorData(GeneratorDataRegistryArguments systestAdaptorArguments)
{
    return systestAdaptorArguments.physicalSourceConfig;
}

}
