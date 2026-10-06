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
#include <VoidSink.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <PipelineState.hpp>

namespace NES
{
std::atomic_uint64_t VoidSink::totalReceivedTuples = 0;
std::atomic_uint64_t VoidSink::lastExportedCount = 0;
std::atomic_uint64_t VoidSink::lastImportedCount = 0;
std::atomic_uint64_t VoidSink::stoppedCount = 0;
std::atomic_uint64_t VoidSink::exports = 0;
std::atomic_uint64_t VoidSink::imports = 0;

VoidSink::VoidSink(BackpressureController backpressureController, const SinkDescriptor&) : Sink(std::move(backpressureController))
{
}

void VoidSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up void sink: {}", *this);
}

void VoidSink::stop(PipelineExecutionContext&)
{
    auto previous = stoppedCount.load();
    while (previous < receivedTuples && not stoppedCount.compare_exchange_weak(previous, receivedTuples))
    {
    }
    NES_INFO("Void Sink completed after {} tuples.", receivedTuples)
}

void VoidSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in VoidSink.");
    const auto count = inputTupleBuffer.getNumberOfTuples();
    receivedTuples += count;
    totalReceivedTuples.fetch_add(count);
}

TupleBuffer VoidSink::emit(PipelineExecutionContext& context)
{
    PipelineStateBuilder state;
    state.append(receivedTuples);
    lastExportedCount.store(receivedTuples);
    exports.fetch_add(1);
    return state.finish(context.getBufferManager());
}

void VoidSink::absorb(const TupleBuffer& state, PipelineExecutionContext&)
{
    PipelineStateReader reader(state);
    receivedTuples = reader.read<uint64_t>();
    reader.ensureConsumed();
    INVARIANT(reader.childCount() == 0, "Unexpected Void sink child state");
    lastImportedCount.store(receivedTuples);
    imports.fetch_add(1);
}

VoidSink::Metrics VoidSink::getMetrics()
{
    return {
        .receivedTuples = totalReceivedTuples.load(),
        .lastExportedCount = lastExportedCount.load(),
        .lastImportedCount = lastImportedCount.load(),
        .stoppedCount = stoppedCount.load(),
        .exports = exports.load(),
        .imports = imports.load()};
}

DescriptorConfig::Config VoidSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersVoid>(std::move(config), NAME);
}

}
