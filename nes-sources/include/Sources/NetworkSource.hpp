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
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <network/lib.h>
#include <rust/cxx.h>

namespace NES
{

/// Source-defined config struct: instantiated from the generic config by the SourceConfig
/// registry entry, carried through the SourceDescriptor as std::any, and serialized via
/// reflection of exactly this struct (all members are reflectable).
struct NetworkSourceConfig
{
    /// UUID identifying the receiver channel of this source.
    std::string channel;
    /// host:port endpoint the receiver network service binds to.
    std::string bind;
    /// Per-channel receiver queue size override. 0 means use the worker-level default.
    size_t receiverQueueSize;

    static std::expected<NetworkSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class NetworkSource final : public Source
{
public:
    constexpr static std::string_view NAME = "Network";

    explicit NetworkSource(const NetworkSourceConfig& config);
    ~NetworkSource() override = default;

    NetworkSource(const NetworkSource&) = delete;
    NetworkSource& operator=(const NetworkSource&) = delete;
    NetworkSource(NetworkSource&&) = delete;
    NetworkSource& operator=(NetworkSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> provider) override;
    void close() override;

    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string channelId;
    size_t receiverQueueSize;
    std::optional<rust::Box<ReceiverDataChannel>> channel;
    rust::Box<ReceiverNetworkService> receiverServer;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

}
