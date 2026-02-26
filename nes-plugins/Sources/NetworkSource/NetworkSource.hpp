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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
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
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "channel",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "bind",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND, config); }};

    /// Per-channel receiver queue size override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> RECEIVER_QUEUE_SIZE{
        "receiver_queue_size",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(RECEIVER_QUEUE_SIZE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, CHANNEL, BIND, RECEIVER_QUEUE_SIZE);
};

/// NOLINTEND(cert-err58-cpp)

}
