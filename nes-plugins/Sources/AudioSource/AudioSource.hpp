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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <netdb.h>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>

namespace NES::Sources
{

/// Defines the names, (optional) default values, (optional) validation & config functions, for all Audio config parameters.
struct ConfigParametersAudio
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> HOST{
        "socketHost",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return Configurations::DescriptorConfig::tryGet(HOST, config); }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> SAMPLE_RATE{
        "sampleRate",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(SAMPLE_RATE, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> SAMPLE_WIDTH{
        "sampleWidth",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto sampleWidth = Configurations::DescriptorConfig::tryGet(SAMPLE_WIDTH, config);
            if (!sampleWidth.has_value())
            {
                return std::nullopt;
            }

            if (sampleWidth != 1 && sampleWidth != 2 && sampleWidth != 4 && sampleWidth != 8)
            {
                NES_ERROR("Invalid Sample width: {}. The Audio source supports only 1,2,4,8", sampleWidth.value());
                return std::nullopt;
            }
            return sampleWidth;
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socketPort",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = Configurations::DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("AudioSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(HOST, SAMPLE_RATE, SAMPLE_WIDTH, PORT);
};

class AudioSource : public Source
{
    constexpr static ssize_t INVALID_RECEIVED_BUFFER_SIZE = -1;
    /// A return value of '0' means an EoF in the context of a read(socket..) (https://man.archlinux.org/man/core/man-pages/read.2.en)
    constexpr static ssize_t EOF_RECEIVED_BUFFER_SIZE = 0;
    /// We implicitly add one microsecond to avoid operation from never timing out
    /// (https://linux.die.net/man/7/socket)
    constexpr static suseconds_t IMPLICIT_TIMEOUT_USEC = 1;
    constexpr static size_t ERROR_MESSAGE_BUFFER_SIZE = 256;

public:
    static const std::string& name()
    {
        static const std::string Instance = "Audio";
        return Instance;
    }

    explicit AudioSource(const SourceDescriptor& sourceDescriptor);
    ~AudioSource() override = default;

    AudioSource(const AudioSource&) = delete;
    AudioSource& operator=(const AudioSource&) = delete;
    AudioSource(AudioSource&&) = delete;
    AudioSource& operator=(AudioSource&&) = delete;

    size_t
    fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider&, const std::stop_token& stopToken) override;

    /// Open Audio connection.
    void open() override;
    /// Close Audio connection.
    void close() override;

    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool tryToConnect(const addrinfo* result, int flags);

    int connection = -1;
    int sockfd = -1;
    std::chrono::microseconds current;

    /// buffer for thread-safe strerror_r
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer;


    std::string socketHost;
    std::string socketPort;
    size_t sampleRate;
    size_t sampleWidth;

    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};

}
