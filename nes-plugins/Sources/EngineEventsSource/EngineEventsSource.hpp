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
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct EngineEventsSourceConfig
{
    /// How long a partially filled TupleBuffer is held back before it is emitted. Bounds the delay between
    /// an event happening and a query seeing it.
    uint32_t flushIntervalMs{};

    static std::expected<EngineEventsSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Reads the query engine's task events, which the worker's 'TaskStatisticListener' publishes as CSV rows,
/// and hands them to the configured InputFormatter. Engine statistics thereby become a stream that
/// NebulaStream can query itself, rather than a trace file to be opened after the fact.
///
/// The feed is identified by the worker this source is placed on, so the source needs no configuration
/// naming it, and the workers that the embedded backend runs inside one process stay separate.
///
/// The source never signals end-of-stream while it is running: an empty feed only means that nothing has
/// happened yet, so it keeps waiting until the stop token fires.
class EngineEventsSource final : public Source
{
public:
    constexpr static std::string_view NAME = "EngineEvents";

    /// The descriptor is needed for the worker the source is placed on, which identifies the feed.
    EngineEventsSource(const EngineEventsSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~EngineEventsSource() override = default;

    EngineEventsSource(const EngineEventsSource&) = delete;
    EngineEventsSource& operator=(const EngineEventsSource&) = delete;
    EngineEventsSource(EngineEventsSource&&) = delete;
    EngineEventsSource& operator=(EngineEventsSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    Host host;
    std::chrono::milliseconds flushInterval;
    /// Holds the feed for as long as the query runs, and releases it for the next query on 'close'.
    std::optional<EventFeedConsumer> consumer;
    /// A row that was popped from the feed but did not fit into the previous TupleBuffer.
    std::optional<std::string> pendingRow;
    size_t emittedRows{0};
};

}
