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

#include <Sources/EventFeedSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <format>
#include <iterator>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string_view>
#include <Feeds/EventFeed.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

#include <Sources/RowBufferPacking.hpp>

namespace NES
{


EventFeedSource::EventFeedSource(
    const SourceDescriptor& sourceDescriptor, const std::string_view feedName, const std::string_view sourceType)
    : host(sourceDescriptor.getHost())
    , feedName(feedName)
    , sourceType(sourceType)
    , flushInterval(std::chrono::milliseconds{sourceDescriptor.getFromConfig(EventFeedSourceConfig::FLUSH_INTERVAL_MS)})
{
}

void EventFeedSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    /// Throws if the worker publishes no such feed, or if another query is already reading it. Both are
    /// user visible mistakes that would otherwise show up as a query that returns nothing forever.
    consumer = EventFeedRegistry::instance().acquire(host, feedName);
    NES_INFO(
        "A {} source is reading the '{}' feed of worker {}, buffered up to {} rows",
        sourceType,
        feedName,
        host.getRawValue(),
        (*consumer)->capacity());
}

void EventFeedSource::close()
{
    if (consumer.has_value())
    {
        NES_INFO(
            "Closing the {} source on worker {} after {} rows, {} rows were dropped",
            sourceType,
            host.getRawValue(),
            emittedRows,
            (*consumer)->droppedRows());
    }
    consumer.reset();
}

Source::FillTupleBufferResult EventFeedSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    return packRowsIntoBuffer(
        tupleBuffer,
        stopToken,
        flushInterval,
        pendingRow,
        emittedRows,
        [this](const std::chrono::milliseconds timeout) { return (*consumer)->tryPop(timeout); },
        fmt::format("the '{}' feed of worker {}", feedName, host.getRawValue()));
}

std::ostream& EventFeedSource::toString(std::ostream& str) const
{
    str << std::format(
        "\n{}Source(host: {}, flushInterval: {}, emittedRows: {})", sourceType, host.getRawValue(), flushInterval, emittedRows);
    return str;
}

}
