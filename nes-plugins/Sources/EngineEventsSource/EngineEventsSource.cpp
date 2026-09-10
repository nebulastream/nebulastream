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

#include <EngineEventsSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <format>
#include <iterator>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

namespace
{
/// Upper bound on how long a single attempt to take a row off the feed waits. While the buffer is still
/// empty this also bounds how quickly the source reacts to a stop request.
constexpr std::chrono::milliseconds POP_TIMEOUT{10};
/// The InputFormatter splits rows on this character, matching the CSV formatter's TUPLE_DELIMITER default.
constexpr char TUPLE_DELIMITER = '\n';
}

EngineEventsSource::EngineEventsSource(const SourceDescriptor& sourceDescriptor)
    : host(sourceDescriptor.getHost())
    , flushInterval(std::chrono::milliseconds{sourceDescriptor.getFromConfig(ConfigParametersEngineEvents::FLUSH_INTERVAL_MS)})
{
}

void EngineEventsSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    /// Throws if the worker publishes no such feed, or if another query is already reading it. Both are
    /// user visible mistakes that would otherwise show up as a query that returns nothing forever.
    consumer = EventFeedRegistry::instance().acquire(host, FeedName::ENGINE_EVENTS);
    NES_INFO(
        "EngineEventsSource is reading the task events of worker {}, buffered up to {} rows", host.getRawValue(), (*consumer)->capacity());
}

void EngineEventsSource::close()
{
    if (consumer.has_value())
    {
        NES_INFO(
            "Closing EngineEventsSource on worker {} after {} rows, {} rows were dropped",
            host.getRawValue(),
            emittedRows,
            (*consumer)->droppedRows());
    }
    consumer.reset();
}

Source::FillTupleBufferResult EngineEventsSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto available = tupleBuffer.getAvailableMemoryArea<char>();
    const auto deadline = std::chrono::steady_clock::now() + flushInterval;
    size_t written = 0;

    while (not stopToken.stop_requested())
    {
        if (not pendingRow.has_value())
        {
            auto popTimeout = POP_TIMEOUT;
            if (written > 0)
            {
                /// Once the buffer holds something, the flush interval is what the caller was promised, so
                /// the pop must not outlive it. Without this a flush interval shorter than POP_TIMEOUT would
                /// be rounded up to the next pop that times out.
                const auto remaining = deadline - std::chrono::steady_clock::now();
                if (remaining <= decltype(remaining)::zero())
                {
                    break;
                }
                popTimeout = std::min(POP_TIMEOUT, std::chrono::ceil<std::chrono::milliseconds>(remaining));
            }
            pendingRow = (*consumer)->tryPop(popTimeout);
        }

        if (pendingRow.has_value())
        {
            /// The delimiter is part of what the InputFormatter has to see, so it counts towards the row size.
            if (const size_t rowSize = pendingRow->size() + 1; rowSize <= available.size() - written)
            {
                std::ranges::copy(*pendingRow, std::next(available.begin(), static_cast<ptrdiff_t>(written)));
                written += pendingRow->size();
                available[written++] = TUPLE_DELIMITER;
                ++emittedRows;
                pendingRow.reset();
            }
            else if (written > 0)
            {
                /// Keep the row for the next buffer.
                break;
            }
            else
            {
                NES_WARNING(
                    "Dropping a row of {} bytes from the task event feed of worker {}, it does not fit into a TupleBuffer of {} bytes",
                    pendingRow->size() + 1,
                    host.getRawValue(),
                    available.size());
                pendingRow.reset();
            }
        }

        /// Returning zero bytes would terminate the source, so an empty feed just means we keep waiting.
        if (written > 0 && std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
    }

    if (written == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(written);
}

std::ostream& EngineEventsSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nEngineEventsSource(host: {}, flushInterval: {}, emittedRows: {})", host.getRawValue(), flushInterval, emittedRows);
    return str;
}

DescriptorConfig::Config EngineEventsSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersEngineEvents>(std::move(config), NAME);
}

}
