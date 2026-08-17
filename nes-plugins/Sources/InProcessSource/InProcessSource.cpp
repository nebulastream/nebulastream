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

#include <InProcessSource.hpp>

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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

namespace
{
/// How long a single attempt to take a row off the feed waits. Also bounds how quickly the source reacts
/// to a stop request while the feed is silent.
constexpr std::chrono::milliseconds POP_TIMEOUT{10};
/// The InputFormatter splits rows on this character, matching the CSV formatter's TUPLE_DELIMITER default.
constexpr char TUPLE_DELIMITER = '\n';
}

InProcessSource::InProcessSource(const SourceDescriptor& sourceDescriptor)
    : feedName(sourceDescriptor.getFromConfig(ConfigParametersInProcess::FEED_NAME))
    , capacity(sourceDescriptor.getFromConfig(ConfigParametersInProcess::CAPACITY))
    , flushInterval(std::chrono::milliseconds{sourceDescriptor.getFromConfig(ConfigParametersInProcess::FLUSH_INTERVAL_MS)})
{
}

void InProcessSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    feed = InProcessFeedRegistry::instance().getOrCreate(feedName, capacity);
    NES_INFO("InProcessSource is reading from feed '{}' with a capacity of {} rows", feedName, feed->capacity());
}

void InProcessSource::close()
{
    NES_INFO("Closing InProcessSource on feed '{}' after {} rows, {} rows were dropped", feedName, emittedRows, feed->droppedRows());
    feed.reset();
}

Source::FillTupleBufferResult InProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto available = tupleBuffer.getAvailableMemoryArea<char>();
    const auto deadline = std::chrono::steady_clock::now() + flushInterval;
    size_t written = 0;

    while (not stopToken.stop_requested())
    {
        if (not pendingRow.has_value())
        {
            pendingRow = feed->tryPop(POP_TIMEOUT);
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
                    "Dropping a row of {} bytes from feed '{}', it does not fit into a TupleBuffer of {} bytes",
                    pendingRow->size() + 1,
                    feedName,
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

std::ostream& InProcessSource::toString(std::ostream& str) const
{
    str << std::format("\nInProcessSource(feed: {}, flushInterval: {}, emittedRows: {})", feedName, flushInterval, emittedRows);
    return str;
}

DescriptorConfig::Config InProcessSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersInProcess>(std::move(config), NAME);
}

}
