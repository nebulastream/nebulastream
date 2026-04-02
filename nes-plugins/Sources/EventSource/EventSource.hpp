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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Tracing/EventStore.hpp>

namespace NES
{

/// EventSource reads events from the in-process EventStore.
/// Used by system queries to consume LOG_EVENT data.
class EventSource : public Source
{
public:
    constexpr static std::string_view NAME = "Event";

    explicit EventSource(const SourceDescriptor& sourceDescriptor);
    ~EventSource() override;

    EventSource(const EventSource&) = delete;
    EventSource& operator=(const EventSource&) = delete;
    EventSource(EventSource&&) = delete;
    EventSource& operator=(EventSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    static size_t computeRowSize(const std::vector<DataType>& fieldTypes);
    static size_t sizeForType(DataType::Type type);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string eventName;
    EventType eventType;
    std::chrono::milliseconds flushInterval;
    std::vector<DataType> fieldTypes;
    size_t rowSize;
    bool includeTimestamp = false;

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<EventSubscriber> subscriber;
    uint64_t eventsReceived = 0;
};

struct ConfigParametersEvent
{
    static inline const DescriptorConfig::ConfigParameter<std::string> EVENT_NAME{
        "event_name",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(EVENT_NAME, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        100,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> INCLUDE_TIMESTAMP{
        "include_timestamp",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INCLUDE_TIMESTAMP, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, EVENT_NAME, FLUSH_INTERVAL_MS, INCLUDE_TIMESTAMP);
};
}
