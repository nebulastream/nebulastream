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

#include <EventSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Tracing/EventStore.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

size_t EventSource::sizeForType(DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UINT8:
        case DataType::Type::INT8:
            return 1;
        case DataType::Type::UINT16:
        case DataType::Type::INT16:
            return 2;
        case DataType::Type::UINT32:
        case DataType::Type::INT32:
        case DataType::Type::FLOAT32:
            return 4;
        case DataType::Type::UINT64:
        case DataType::Type::INT64:
        case DataType::Type::FLOAT64:
            return 8;
        case DataType::Type::VARSIZED:
            return sizeof(VariableSizedAccess);
        default:
            return 0;
    }
}

size_t EventSource::computeRowSize(const std::vector<DataType>& types)
{
    size_t total = 0;
    for (const auto& ft : types)
    {
        total += sizeForType(ft.type);
    }
    return total;
}

EventSource::EventSource(const SourceDescriptor& sourceDescriptor)
    : eventName(sourceDescriptor.getFromConfig(ConfigParametersEvent::EVENT_NAME))
    , flushInterval(std::chrono::milliseconds{sourceDescriptor.getFromConfig(ConfigParametersEvent::FLUSH_INTERVAL_MS)})
    , includeTimestamp(sourceDescriptor.getFromConfig(ConfigParametersEvent::INCLUDE_TIMESTAMP))
{
    auto resolved = EventStore::fromName(eventName);
    PRECONDITION(resolved.has_value(), "EventSource: unknown event type '{}'", eventName);
    eventType = resolved.value();

    const auto& schema = *sourceDescriptor.getLogicalSource().getSchema();
    const size_t numFields = schema.getNumberOfFields();
    const size_t numArgs = includeTimestamp ? numFields - 1 : numFields;

    PRECONDITION(numArgs >= 1 && numArgs <= 8, "EventSource requires 1-{} args, got {}", 8, numArgs);

    fieldTypes.reserve(numFields);
    for (size_t i = 0; i < numFields; ++i)
    {
        const auto& field = schema.getFieldAt(i);
        const auto sz = sizeForType(field.dataType.type);
        PRECONDITION(sz > 0, "EventSource: unsupported data type for field '{}': {}", field.name, field.dataType);
        fieldTypes.push_back(field.dataType);
    }

    rowSize = computeRowSize(fieldTypes);
    NES_INFO(
        "EventSource: event='{}', fields={}, args={}, rowSize={} bytes, timestamp={}",
        eventName,
        numFields,
        numArgs,
        rowSize,
        includeTimestamp);
}

EventSource::~EventSource()
{
    close();
}

void EventSource::open(std::shared_ptr<AbstractBufferProvider> bp)
{
    bufferProvider = std::move(bp);
    subscriber = std::make_shared<EventSubscriber>();
    EventStore::instance().subscribe(eventType, subscriber);
    eventsReceived = 0;
    NES_INFO("EventSource: subscribed to event '{}'", eventName);
}

void EventSource::close()
{
    if (subscriber)
    {
        EventStore::instance().unsubscribe(eventType);
        subscriber.reset();
    }
    bufferProvider.reset();
    NES_INFO("EventSource: closed, total events received: {}", eventsReceived);
}

/// Helper to write a uint64 field, converting ns→ms if it's the timestamp.
static void writeU64(uint8_t*& writePtr, uint64_t val)
{
    std::memcpy(writePtr, &val, 8);
    writePtr += 8;
}

/// Helper to write a VARSIZED string field into the tuple buffer + child buffer.
static void writeString(
    uint8_t*& writePtr,
    const char* str,
    TupleBuffer& tupleBuffer,
    std::shared_ptr<AbstractBufferProvider>& bufProv,
    uint32_t& childOffset,
    bool& hasChild,
    TupleBuffer& childBuffer)
{
    const size_t len = std::strlen(str);

    if (!hasChild)
    {
        childBuffer = bufProv->getBufferBlocking();
        hasChild = true;
        childOffset = 0;
    }

    if (childOffset + len > childBuffer.getBufferSize())
    {
        childBuffer.setNumberOfTuples(childOffset);
        [[maybe_unused]] auto idx = tupleBuffer.storeChildBuffer(childBuffer);
        childBuffer = bufProv->getBufferBlocking();
        childOffset = 0;
    }

    auto childMem = childBuffer.getAvailableMemoryArea<uint8_t>();
    std::memcpy(childMem.data() + childOffset, str, len);

    auto childIdx = VariableSizedAccess::Index(tupleBuffer.getNumberOfChildBuffers());
    auto varAccess = VariableSizedAccess(childIdx, VariableSizedAccess::Offset(childOffset), VariableSizedAccess::Size(len));
    std::memcpy(writePtr, &varAccess, sizeof(VariableSizedAccess));
    writePtr += sizeof(VariableSizedAccess);

    childOffset += len;
}

/// Visitor that writes a typed event variant to the tuple buffer.
struct EventWriter
{
    uint8_t* writePtr;
    bool includeTimestamp;
    TupleBuffer& tupleBuffer;
    std::shared_ptr<AbstractBufferProvider>& bufProv;
    uint32_t& childOffset;
    bool& hasChild;
    TupleBuffer& childBuffer;

    void operator()(const PipelineCompiledEvent& ev)
    {
        if (includeTimestamp)
            writeU64(writePtr, ev.timestamp_ns / 1'000'000);
        writeU64(writePtr, ev.query_id_hi);
        writeU64(writePtr, ev.query_id_lo);
        writeU64(writePtr, ev.pipeline_id);
        writeU64(writePtr, ev.compile_time_ns);
    }

    void operator()(const QueryStatusEvent& ev)
    {
        if (includeTimestamp)
            writeU64(writePtr, ev.timestamp_ns / 1'000'000);
        writeU64(writePtr, ev.query_id_hi);
        writeU64(writePtr, ev.query_id_lo);
        writeString(writePtr, ev.status, tupleBuffer, bufProv, childOffset, hasChild, childBuffer);
    }

    void operator()(const UnpooledBufferAllocEvent& ev)
    {
        if (includeTimestamp)
            writeU64(writePtr, ev.timestamp_ns / 1'000'000);
        writeU64(writePtr, ev.alloc_size);
    }

    void operator()(const BufferIngestionEvent& ev)
    {
        if (includeTimestamp)
            writeU64(writePtr, ev.timestamp_ns / 1'000'000);
        writeU64(writePtr, ev.query_id_hi);
        writeU64(writePtr, ev.query_id_lo);
        writeU64(writePtr, ev.origin_id);
        writeU64(writePtr, ev.num_tuples);
    }
};

Source::FillTupleBufferResult EventSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const size_t bufferCapacity = tupleBuffer.getBufferSize();
    auto memArea = tupleBuffer.getAvailableMemoryArea<uint8_t>();

    size_t bytesWritten = 0;
    uint64_t eventsConsumed = 0;
    uint32_t childOffset = 0;
    bool hasChild = false;
    TupleBuffer childBuffer;

    /// Drain events from the queue. Uses tryReadUntil with a 10ms deadline so we
    /// check the stop token regularly without busy-spinning when idle.
    while (!stopToken.stop_requested())
    {
        LogEventVariant ev;
        if (subscriber->tryReadUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(10), ev))
        {
            do
            {
                if (bytesWritten + rowSize > bufferCapacity)
                {
                    goto flush;
                }
                EventWriter writer{
                    memArea.data() + bytesWritten, includeTimestamp, tupleBuffer, bufferProvider, childOffset, hasChild, childBuffer};
                std::visit(writer, ev);
                bytesWritten += rowSize;
                eventsConsumed++;
            } while (subscriber->tryPop(ev));

            break; /// got data, emit the buffer
        }
    }

flush:
    eventsReceived += eventsConsumed;

    if (hasChild)
    {
        childBuffer.setNumberOfTuples(childOffset);
        [[maybe_unused]] auto idx = tupleBuffer.storeChildBuffer(childBuffer);
    }

    if (bytesWritten == 0)
    {
        return FillTupleBufferResult::eos();
    }

    NES_TRACE("EventSource: wrote {} bytes ({} events) to tuple buffer", bytesWritten, eventsConsumed);
    return FillTupleBufferResult::withBytes(eventsConsumed);
}

std::ostream& EventSource::toString(std::ostream& str) const
{
    str << "\nEventSource(";
    str << "\n\tevent_name: " << eventName;
    str << "\n\tevents_received: " << eventsReceived;
    str << "\n\trow_size: " << rowSize;
    str << ")\n";
    return str;
}

DescriptorConfig::Config EventSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersEvent>(std::move(config), NAME);
}

SourceValidationRegistryReturnType
/// NOLINTNEXTLINE(performance-unnecessary-value-param)
RegisterEventSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return EventSource::validateAndFormat(sourceConfig.config);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterEventSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<EventSource>(sourceRegistryArguments.sourceDescriptor);
}

}
