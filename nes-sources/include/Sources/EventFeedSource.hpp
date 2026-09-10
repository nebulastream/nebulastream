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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Turns one of the worker's 'EventFeed's into an ordinary stream: it pops the rows the worker publishes
/// and hands them to the configured InputFormatter. What the worker observes about itself thereby becomes
/// something NebulaStream can query, rather than a trace file to be opened after the fact.
///
/// Which feed a source reads follows from its type, not from its configuration, so a query cannot misspell
/// a feed name and no user facing option has to name one. Subclasses exist to make that choice, see
/// 'EngineEventsSource' and 'BufferEventsSource'.
///
/// The feed is additionally identified by the worker this source is placed on, so the workers that the
/// embedded backend runs inside one process stay separate.
///
/// The source never signals end-of-stream while it is running: an empty feed only means that nothing has
/// happened yet, so it keeps waiting until the stop token fires.
class EventFeedSource : public Source
{
public:
    /// @param feedName the feed to read, one of the 'FeedName' constants
    /// @param sourceType the source type as a query names it, used for logging and for the plan rendering
    EventFeedSource(const SourceDescriptor& sourceDescriptor, std::string_view feedName, std::string_view sourceType);
    ~EventFeedSource() override = default;

    EventFeedSource(const EventFeedSource&) = delete;
    EventFeedSource& operator=(const EventFeedSource&) = delete;
    EventFeedSource(EventFeedSource&&) = delete;
    EventFeedSource& operator=(EventFeedSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) final;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) final;
    void close() final;

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    Host host;
    std::string_view feedName;
    std::string_view sourceType;
    std::chrono::milliseconds flushInterval;
    /// Holds the feed for as long as the query runs, and releases it for the next query on 'close'.
    std::optional<EventFeedConsumer> consumer;
    /// A row that was popped from the feed but did not fit into the previous TupleBuffer.
    std::optional<std::string> pendingRow;
    size_t emittedRows{0};
};

/// The configuration every event feed source shares. A subclass folds this into its own parameter map, so
/// that the option is documented under the source type a query actually writes.
struct EventFeedSourceConfig
{
    /// How long a partially filled TupleBuffer is held back before it is emitted. Bounds the delay between
    /// an event happening and a query seeing it.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        100,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto value = DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config);
            return value.has_value() && *value > 0 ? value : std::nullopt;
        }};
};

}
