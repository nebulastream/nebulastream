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

#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Validation/EndpointValidation.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <network/lib.h>
#include <rust/cxx.h>

namespace NES
{

class NetworkSource final : public Source
{
public:
    static const std::string& name()
    {
        static const std::string Instance = "Network";
        return Instance;
    }

    explicit NetworkSource(const SourceDescriptor& sourceDescriptor);
    ~NetworkSource() override = default;

    NetworkSource(const NetworkSource&) = delete;
    NetworkSource& operator=(const NetworkSource&) = delete;
    NetworkSource(NetworkSource&&) = delete;
    NetworkSource& operator=(NetworkSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> provider) override;
    void close() override;

    [[nodiscard]] bool addsMetadata() const override { return true; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes);

    std::string channelId;
    size_t receiverQueueSize;
    std::optional<rust::Box<ReceiverDataChannel>> channel;
    rust::Box<ReceiverNetworkService> receiverServer;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersNetworkSource
{
    static inline const ConfigField<std::string> CHANNEL{
        "CHANNEL",
        [](const ConfigLiteral& literal)
        {
            return NES::tryGetOr<std::string>(literal, NES::expectedType<std::string>())
                .and_then(
                    [](const std::string& value) -> std::expected<std::string, Exception>
                    {
                        if (!stringToUUID(value))
                        {
                            return std::unexpected{InvalidConfigParameter("NetworkSource: channel must be a valid UUID, got: {}", value)};
                        }
                        return value;
                    });
        }};

    static inline const ConfigField<std::string> BIND{
        "BIND",
        [](const ConfigLiteral& config)
        {
            return NES::tryGetOr<std::string>(config, NES::expectedType<std::string>())
                .and_then(
                    [](const std::string& value) -> std::expected<std::string, Exception>
                    {
                        if (!EndpointValidation{}.isValid(value))
                        {
                            return std::unexpected{InvalidConfigParameter("NetworkSource: bind must be host:port format, got: {}", value)};
                        }
                        return value;
                    });
        }};

    /// Per-channel receiver queue size override. 0 means use the worker-level default.
    /// When a user explicitly sets receiver_queue_size=0, the lambda rejects it with an error.
    /// The default value (0) is returned directly by the config system, bypassing the lambda.
    static inline const ConfigField<size_t> RECEIVER_QUEUE_SIZE{
        "RECEIVER_QUEUE_SIZE",
        [](const ConfigLiteral& config)
        {
            return NES::tryGetOr<size_t>(config, NES::expectedType<size_t>())
                .and_then(
                    [](const size_t& value) -> std::expected<size_t, Exception>
                    {
                        if (value == 0)
                        {
                            return std::unexpected{
                                InvalidConfigParameter("NetworkSource: receiver_queue_size must be > 0 when explicitly set")};
                        }
                        return value;
                    });
        },
        size_t{0},
    };

    static inline auto configSchema = createConfigSchema(Identifier::parse("NETWORK_SOURCE"), CHANNEL, BIND, RECEIVER_QUEUE_SIZE);
};

/// NOLINTEND(cert-err58-cpp)

}
