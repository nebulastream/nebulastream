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

#include "Sources/Source.hpp"

#include <iostream>
#include <optional>
#include <string>

#include <mqtt/async_client.h>

#include <Configurations/Descriptor.hpp>

namespace NES::Sources
{


class MQTTSource : public Source
{
public:
    static inline const std::string NAME = "MQTT";

    enum class QualityOfService : uint8_t
    {
        AT_MOST_ONCE = 0,
        AT_LEAST_ONCE = 1,
        EXACTLY_ONCE = 2
    };
    friend std::ostream& operator<<(std::ostream& str, const QualityOfService& qos)
    {
        switch (qos)
        {
            case QualityOfService::AT_MOST_ONCE:
                return str << "AT_MOST_ONCE";
            case QualityOfService::AT_LEAST_ONCE:
                return str << "AT_LEAST_ONCE";
            case QualityOfService::EXACTLY_ONCE:
                return str << "EXACTLY_ONCE";
            default:
                return str << "UNKNOWN";
        }
    }

    explicit MQTTSource(const SourceDescriptor& sourceDescriptor);
    ~MQTTSource() override = default;

    MQTTSource(const MQTTSource&) = delete;
    MQTTSource& operator=(const MQTTSource&) = delete;
    MQTTSource(MQTTSource&&) = delete;
    MQTTSource& operator=(MQTTSource&&) = delete;

    /// Open connection to MQTT broker.
    void open() override;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) override;

    /// Close connection to MQTT broker.
    void close() override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string serverURI;
    std::string clientId;
    std::string topic;
    QualityOfService qos;

    std::unique_ptr<mqtt::async_client> client;

    /// A simple stash for persisting partial payloads that did not fit into a TB between calls to `fillTupleBuffer`.
    /// When a payload does not fit, stash the remainder.
    /// On the next invocation, consume the stashed payload before reading new data.
    /// The stash can be in two states:
    /// 1. Empty: consumed == tail == 0
    /// 2. Partly Consumed: consumed < tail
    ///
    /// The stash has the following two operations:
    /// 1. Stash a new payload by placing it into the internal buffer. This operation is allowed only when the stash is in empty state.
    /// 2. Consume up to `numBytes` from the stashed payload. This operation is allowed only when the stash is in partly consumed state.
    ///
    /// The consume method takes care of transferring the stash into the empty state when the whole payload has been consumed.
    /// The caller has to take care of consuming before stashing more data.
    class PayloadStash
    {
    public:
        PayloadStash() { buf.reserve(STASH_TARGET_CAPACITY); }
        ~PayloadStash() = default;

        /// No copying or moving necessary.
        [[nodiscard]] PayloadStash(const PayloadStash&) = delete;
        PayloadStash(PayloadStash&&) = delete;
        PayloadStash& operator=(const PayloadStash&) = delete;
        PayloadStash& operator=(PayloadStash&&) = delete;

        /// When the consumed pointer equals the tail pointer, everything has been consumed.
        [[nodiscard]] bool empty() const { return consumed == tail; }

        /// Consume up to `numBytes` from the stashed payload.
        /// This is either the capacity of a TB, or the remaining part of the stashed payload, whatever is smaller.
        /// @return a view to the consumed payload
        [[nodiscard]] std::string_view consume(size_t numBytes)
        {
            PRECONDITION(numBytes > 0, "numBytes must be a positive integer.");
            INVARIANT(consumed < tail)

            numBytes = std::min(numBytes, tail - consumed);
            auto consumeView = std::string_view{buf}.substr(consumed, numBytes);
            consumed += numBytes;

            /// If we consumed the whole payload, reset the stash
            if (consumed == tail)
            {
                reset();
            }
            return consumeView;
        }

        /// Stash a new payload by copying it into the internal buffer.
        /// Reset the consumed pointer and set the tail pointer to the end of the payload.
        void stash(std::string_view payload)
        {
            PRECONDITION(payload.size() > 0, "Payload must not be empty.");
            INVARIANT(consumed == 0 && tail == 0, "Consume stash before stashing a new payload.");

            buf.assign(payload);
            consumed = 0;
            tail = payload.size();

            /// If the last payload increased over the default capacity, shrink it back.
            if (buf.capacity() > STASH_TARGET_CAPACITY)
            {
                buf.shrink_to_fit();
            }
        }

    private:
        void reset()
        {
            consumed = 0;
            tail = 0;
        }

        const size_t STASH_TARGET_CAPACITY = 1024;

        std::string buf;
        size_t consumed = 0;
        size_t tail = 0;
    };

    PayloadStash payloadStash;

    void stashPayloadRemainder(std::string_view remainder);

    size_t writePayloadToBuffer(std::string_view payload, NES::Memory::TupleBuffer& tb, size_t tbOffset);
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all MQTT config parameters.
struct ConfigParametersMQTT
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serverURI", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(SERVER_URI, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(TOPIC, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, MQTTSource::QualityOfService> QOS{
        "qos", MQTTSource::QualityOfService::AT_LEAST_ONCE, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(QOS, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap();
};


}
