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
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/CountAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/SumAggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <PipelineState.hpp>
#include <PipelineStateBufferRef.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
void requireFixedAggregationState(const AggregationPhysicalFunction& function)
{
    INVARIANT(
        dynamic_cast<const CountAggregationPhysicalFunction*>(&function) || dynamic_cast<const SumAggregationPhysicalFunction*>(&function),
        "Window migration currently supports only COUNT and SUM aggregation states");
}

uint64_t checkedRecordCount(const TupleBuffer& buffer)
{
    const auto map = ChainedHashMap::load(buffer);
    INVARIANT(map.getNumberOfVarSizedPages() == 0, "Variable-size hash map state is not supported by window migration");
    INVARIANT(buffer.getNumberOfChildBuffers() <= 1, "Aggregation state has child buffers that require a dedicated codec");
    return map.getTotalNumberOfRecords();
}
}

struct AggregationEmitSession
{
    DefaultTimeBasedSliceStore::Snapshot store;
    WindowBasedOperatorHandler::WatermarkSnapshot watermarks;
    std::shared_ptr<AbstractBufferProvider> provider;

    [[nodiscard]] const AggregationSlice& slice(const uint64_t index) const
    {
        return dynamic_cast<const AggregationSlice&>(*store.slices.at(index));
    }

    [[nodiscard]] const TupleBuffer* mapBuffer(const uint64_t sliceIndex, const uint64_t worker) const
    {
        return slice(sliceIndex).getHashMapBufferRefForWorker(WorkerThreadId(worker));
    }

    [[nodiscard]] uint64_t byteCount(const uint64_t payloadSize) const
    {
        uint64_t bytes = 8 * (4 + 2 * (watermarks.build.size() + watermarks.probe.size()));
        for (uint64_t sliceIndex = 0; sliceIndex < store.slices.size(); ++sliceIndex)
        {
            bytes += 3 * 8;
            for (uint64_t worker = 0; worker < slice(sliceIndex).getNumberOfHashMaps(); ++worker)
            {
                if (const auto* buffer = mapBuffer(sliceIndex, worker))
                {
                    bytes += checkedRecordCount(*buffer) * (8 + payloadSize);
                }
            }
        }
        bytes += store.windows.size() * 3 * 8;
        return bytes;
    }
};

struct AggregationAbsorbSession
{
    AggregationOperatorHandler& handler;
    DefaultTimeBasedSliceStore& store;
    const ChainedHashMapConfig& config;
    std::shared_ptr<AbstractBufferProvider> provider;
    uint64_t workerCount;
    std::vector<std::shared_ptr<Slice>> slices;
    std::vector<DefaultTimeBasedSliceStore::WindowSnapshot> windows;
    WindowBasedOperatorHandler::WatermarkSnapshot watermarks;
    SequenceNumber::Underlying nextSequence = 0;

    void setOrigin(const uint64_t processor, const uint64_t index, const uint64_t sequence, const uint64_t watermark)
    {
        auto& origins = processor == 0 ? watermarks.build : watermarks.probe;
        auto& origin = origins.at(index);
        origin.sequence = sequence;
        origin.watermark = watermark;
    }

    void addSlice(const uint64_t start, const uint64_t end)
    {
        const CreateNewHashMapSliceArgs args(config, *provider);
        slices.push_back(std::make_shared<AggregationSlice>(SliceStart(start), SliceEnd(end), args, workerCount));
    }

    [[nodiscard]] const TupleBuffer* ensureMap(const uint64_t sliceIndex)
    {
        auto& slice = dynamic_cast<AggregationSlice&>(*slices.at(sliceIndex));
        return slice.getOrCreateHashMapBufferRefForWorker(*provider, WorkerThreadId(0));
    }

    [[nodiscard]] int8_t* insert(const uint64_t sliceIndex, const uint64_t hash)
    {
        auto map = ChainedHashMap::load(*ensureMap(sliceIndex));
        auto* entry = map.insertEntry(
            hash,
            provider.get(),
            config.entrySize,
            config.entriesPerPage(),
            config.pageSize,
            ChainedHashMap::calculateMask(config.numberOfBuckets));
        return reinterpret_cast<int8_t*>(entry) + sizeof(ChainedHashMapEntry);
    }

    void addWindow(const uint64_t start, const uint64_t end, const uint64_t state)
    {
        INVARIANT(state <= static_cast<uint64_t>(WindowInfoState::EMITTED_TO_PROBE), "Invalid migrated window status");
        windows.push_back({WindowInfo(start, end), static_cast<WindowInfoState>(state)});
    }

    void finish()
    {
        handler.restoreWatermarkState(watermarks);
        store.restore(std::move(slices), windows, nextSequence);
    }
};

struct AggregationEmitRuntime
{
    AggregationEmitSession session;
    TupleBuffer buffer;
    uint64_t usedBytes = 0;
};

struct AggregationAbsorbRuntime
{
    TupleBuffer buffer;
    AggregationAbsorbSession session;
};

const auto& emitOrigin(const AggregationEmitSession* session, const uint64_t processor, const uint64_t index)
{
    return (processor == 0 ? session->watermarks.build : session->watermarks.probe).at(index);
}

uint64_t emitOriginSequence(const AggregationEmitSession* session, const uint64_t processor, const uint64_t index)
{
    return emitOrigin(session, processor, index).sequence;
}

uint64_t emitOriginWatermark(const AggregationEmitSession* session, const uint64_t processor, const uint64_t index)
{
    return emitOrigin(session, processor, index).watermark;
}

uint64_t originCount(const AggregationAbsorbSession* session, const uint64_t processor)
{
    return processor == 0 ? session->watermarks.build.size() : session->watermarks.probe.size();
}

uint64_t emitSliceStart(const AggregationEmitSession* session, const uint64_t index)
{
    return session->slice(index).getSliceStart().getRawValue();
}

uint64_t emitSliceEnd(const AggregationEmitSession* session, const uint64_t index)
{
    return session->slice(index).getSliceEnd().getRawValue();
}

uint64_t emitWindowStart(const AggregationEmitSession* session, const uint64_t index)
{
    return session->store.windows.at(index).info.windowStart.getRawValue();
}

uint64_t emitWindowEnd(const AggregationEmitSession* session, const uint64_t index)
{
    return session->store.windows.at(index).info.windowEnd.getRawValue();
}

uint64_t emitWindowStatus(const AggregationEmitSession* session, const uint64_t index)
{
    return static_cast<uint64_t>(session->store.windows.at(index).state);
}

const TupleBuffer* mapBuffer(const AggregationEmitSession* session, const uint64_t sliceIndex, const uint64_t worker)
{
    return session->mapBuffer(sliceIndex, worker);
}

uint64_t mapRecords(const TupleBuffer* buffer)
{
    return checkedRecordCount(*buffer);
}

uint64_t mapPageCount(const TupleBuffer* buffer)
{
    return ChainedHashMap::load(*buffer).getNumberOfPages();
}

uint64_t mapPageRecordCount(const TupleBuffer* buffer, const uint64_t page)
{
    return ChainedHashMap::load(*buffer).getPage(page).getNumberOfTuples();
}

ChainedHashMapEntry* mapPageData(const TupleBuffer* buffer, const uint64_t page)
{
    auto pageBuffer = ChainedHashMap::load(*buffer).getPage(page);
    return reinterpret_cast<ChainedHashMapEntry*>(pageBuffer.getAvailableMemoryArea().data());
}

uint64_t emitNextSequence(const AggregationEmitSession* session)
{
    return session->store.nextSequence;
}

uint64_t emitOriginCount(const AggregationEmitSession* session, const uint64_t processor)
{
    return processor == 0 ? session->watermarks.build.size() : session->watermarks.probe.size();
}

uint64_t emitLastForwardedWatermark(const AggregationEmitSession* session)
{
    return session->watermarks.lastForwarded;
}

uint64_t emitSliceCount(const AggregationEmitSession* session)
{
    return session->store.slices.size();
}

AbstractBufferProvider* emitBufferProvider(const AggregationEmitSession* session)
{
    return session->provider.get();
}

void initializeCombinedAggregationMap(
    AbstractBufferProvider* provider,
    TupleBuffer* destination,
    const uint64_t entrySize,
    const uint64_t buckets,
    const uint64_t pageSize,
    const uint64_t bloomBytes)
{
    auto buffer = provider->getUnpooledBuffer(ChainedHashMap::calculateBufferSize(buckets, bloomBytes));
    INVARIANT(buffer.has_value(), "Could not allocate combined aggregation map");
    *destination = std::move(*buffer);
    ChainedHashMap::init(*destination, entrySize, buckets, pageSize, bloomBytes);
}

uint64_t emitSliceWorkerCount(const AggregationEmitSession* session, const uint64_t sliceIndex)
{
    return session->slice(sliceIndex).getNumberOfHashMaps();
}

uint64_t emitWindowCount(const AggregationEmitSession* session)
{
    return session->store.windows.size();
}

void absorbNextSequence(AggregationAbsorbSession* session, const uint64_t sequence)
{
    session->nextSequence = sequence;
}

void absorbOrigin(
    AggregationAbsorbSession* session, const uint64_t processor, const uint64_t index, const uint64_t sequence, const uint64_t watermark)
{
    session->setOrigin(processor, index, sequence, watermark);
}

void absorbLastForwardedWatermark(AggregationAbsorbSession* session, const uint64_t watermark)
{
    session->watermarks.lastForwarded = watermark;
}

void absorbSlice(AggregationAbsorbSession* session, const uint64_t start, const uint64_t end)
{
    session->addSlice(start, end);
}

int8_t* absorbEntry(AggregationAbsorbSession* session, const uint64_t sliceIndex, const uint64_t hash)
{
    return session->insert(sliceIndex, hash);
}

void absorbWindow(AggregationAbsorbSession* session, const uint64_t start, const uint64_t end, const uint64_t status)
{
    session->addWindow(start, end, status);
}

void finishAggregationAbsorb(AggregationAbsorbSession* session)
{
    session->finish();
}

void throwMissingAggregationDonor()
{
    throw NotImplemented("Window aggregation was not lowered with a donor state definition");
}

TupleBuffer* emitRuntimeBuffer(AggregationEmitRuntime* runtime)
{
    return &runtime->buffer;
}

const AggregationEmitSession* emitRuntimeSession(AggregationEmitRuntime* runtime)
{
    return &runtime->session;
}

uint64_t* emitRuntimeUsedBytes(AggregationEmitRuntime* runtime)
{
    return &runtime->usedBytes;
}

const TupleBuffer* absorbRuntimeBuffer(AggregationAbsorbRuntime* runtime)
{
    return &runtime->buffer;
}

AggregationAbsorbSession* absorbRuntimeSession(AggregationAbsorbRuntime* runtime)
{
    return &runtime->session;
}

PipelineStateReader* aggregationChildReader(PipelineStateReader* parent)
{
    INVARIANT(parent->childCount() == 2, "Expected aggregation and downstream state children");
    return parent->own(std::make_unique<PipelineStateReader>(parent->child(1)));
}

void finishAggregationChildReader(PipelineStateReader* parent, PipelineStateReader* child)
{
    child->ensureConsumed();
    parent->ensureConsumed();
}

void finishAggregationLeafReader(PipelineStateReader* state)
{
    INVARIANT(state->childCount() == 1, "Expected aggregation state child");
    state->ensureConsumed();
}

void AggregationBuildPhysicalOperator::traceEmitState(
    nautilus::val<TupleBuffer*> output, nautilus::val<const AggregationEmitSession*> session, nautilus::val<uint64_t*> usedBytes) const
{
    const auto payloadSize = hashMapConfig.entrySize - sizeof(ChainedHashMapEntry);
    const auto& config = hashMapConfig;
    const auto& functions = aggregationPhysicalFunctions;
    PipelineStateBufferRef state(BorrowedNautilusBuffer::from(output));
    state.writeU64(nautilus::invoke(emitNextSequence, session));
    for (nautilus::val<uint64_t> processor = 0; processor < 2; ++processor)
    {
        const auto count = nautilus::invoke(emitOriginCount, session, processor);
        for (nautilus::val<uint64_t> index = 0; index < count; ++index)
        {
            state.writeU64(nautilus::invoke(emitOriginSequence, session, processor, index));
            state.writeU64(nautilus::invoke(emitOriginWatermark, session, processor, index));
        }
    }
    state.writeU64(nautilus::invoke(emitLastForwardedWatermark, session));
    const auto slices = nautilus::invoke(emitSliceCount, session);
    state.writeU64(slices);
    for (nautilus::val<uint64_t> sliceIndex = 0; sliceIndex < slices; ++sliceIndex)
    {
        state.writeU64(nautilus::invoke(emitSliceStart, session, sliceIndex));
        state.writeU64(nautilus::invoke(emitSliceEnd, session, sliceIndex));
        OwnedNautilusBuffer combinedBuffer;
        const auto provider = nautilus::invoke(emitBufferProvider, session);
        nautilus::invoke(
            initializeCombinedAggregationMap,
            provider,
            combinedBuffer.asArg(),
            nautilus::val<uint64_t>{config.entrySize},
            nautilus::val<uint64_t>{config.numberOfBuckets},
            nautilus::val<uint64_t>{config.pageSize},
            nautilus::val<uint64_t>{config.bloomFilterMemAreaSize()});
        const BorrowedNautilusBuffer combinedRef = combinedBuffer;
        ChainedHashMapRef combinedMap{combinedRef, config};
        PipelineMemoryProvider memory(nautilus::val<Arena*>{nullptr}, provider);
        const auto workers = nautilus::invoke(emitSliceWorkerCount, session, sliceIndex);
        for (nautilus::val<uint64_t> worker = 0; worker < workers; ++worker)
        {
            const auto buffer = nautilus::invoke(mapBuffer, session, sliceIndex, worker);
            if (buffer != nullptr)
            {
                const auto sourceRef = BorrowedNautilusBuffer::from(buffer);
                const auto pages = nautilus::invoke(mapPageCount, buffer);
                for (nautilus::val<uint64_t> page = 0; page < pages; ++page)
                {
                    const auto pageData = nautilus::invoke(mapPageData, buffer, page);
                    const auto records = nautilus::invoke(mapPageRecordCount, buffer, page);
                    for (nautilus::val<uint64_t> record = 0; record < records; ++record)
                    {
                        const auto sourceEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(
                            static_cast<nautilus::val<int8_t*>>(pageData) + record * config.entrySize);
                        const ChainedHashMapRef::ChainedEntryRef source{sourceEntry, sourceRef, config.fieldKeys, config.fieldValues};
                        nautilus::val<AbstractHashMapEntry*> destinationEntry = nullptr;
                        combinedMap.insertOrUpdateEntry(
                            source.entryRef,
                            [&destinationEntry](const nautilus::val<AbstractHashMapEntry*>& entry) { destinationEntry = entry; },
                            [&functions, &memory, combinedRef, &config, &destinationEntry](
                                const nautilus::val<AbstractHashMapEntry*>& entry)
                            {
                                const ChainedHashMapRef::ChainedEntryRef destination{
                                    entry, combinedRef, config.fieldKeys, config.fieldValues};
                                auto targetState = static_cast<nautilus::val<AggregationState*>>(destination.getValueMemArea());
                                for (const auto& function : nautilus::static_iterable(functions))
                                {
                                    function->reset(targetState, combinedRef, memory);
                                    targetState = targetState + function->getSizeOfStateInBytes();
                                }
                                destinationEntry = entry;
                            },
                            provider);
                        const ChainedHashMapRef::ChainedEntryRef destination{
                            destinationEntry, combinedRef, config.fieldKeys, config.fieldValues};
                        auto targetState = static_cast<nautilus::val<AggregationState*>>(destination.getValueMemArea());
                        auto sourceState = static_cast<nautilus::val<AggregationState*>>(source.getValueMemArea());
                        for (const auto& function : nautilus::static_iterable(functions))
                        {
                            function->combine(targetState, combinedRef, sourceState, sourceRef, memory);
                            targetState = targetState + function->getSizeOfStateInBytes();
                            sourceState = sourceState + function->getSizeOfStateInBytes();
                        }
                    }
                }
            }
        }
        state.writeU64(nautilus::invoke(mapRecords, combinedBuffer.asArg()));
        const auto combinedPages = nautilus::invoke(mapPageCount, combinedBuffer.asArg());
        for (nautilus::val<uint64_t> page = 0; page < combinedPages; ++page)
        {
            const auto pageData = nautilus::invoke(mapPageData, combinedBuffer.asArg(), page);
            const auto records = nautilus::invoke(mapPageRecordCount, combinedBuffer.asArg(), page);
            for (nautilus::val<uint64_t> record = 0; record < records; ++record)
            {
                const auto entry = static_cast<nautilus::val<ChainedHashMapEntry*>>(
                    static_cast<nautilus::val<int8_t*>>(pageData) + record * config.entrySize);
                state.writeU64(readValueFromMemRef<uint64_t>(getMemberRef(entry, &ChainedHashMapEntry::hash)));
                state.writeBytes(static_cast<nautilus::val<const int8_t*>>(entry) + sizeof(ChainedHashMapEntry), payloadSize);
            }
        }
    }
    const auto windows = nautilus::invoke(emitWindowCount, session);
    state.writeU64(windows);
    for (nautilus::val<uint64_t> index = 0; index < windows; ++index)
    {
        state.writeU64(nautilus::invoke(emitWindowStart, session, index));
        state.writeU64(nautilus::invoke(emitWindowEnd, session, index));
        state.writeU64(nautilus::invoke(emitWindowStatus, session, index));
    }
    *usedBytes = state.position();
}

void AggregationBuildPhysicalOperator::traceAbsorbState(
    nautilus::val<const TupleBuffer*> input, nautilus::val<AggregationAbsorbSession*> session) const
{
    const auto payloadSize = hashMapConfig.entrySize - sizeof(ChainedHashMapEntry);
    PipelineStateBufferRef state(BorrowedNautilusBuffer::from(input));
    const auto nextSequence = state.readU64();
    nautilus::invoke(absorbNextSequence, session, nextSequence);
    for (nautilus::val<uint64_t> processor = 0; processor < 2; ++processor)
    {
        const auto count = nautilus::invoke(originCount, session, processor);
        for (nautilus::val<uint64_t> index = 0; index < count; ++index)
        {
            const auto sequence = state.readU64();
            const auto watermark = state.readU64();
            nautilus::invoke(absorbOrigin, session, processor, index, sequence, watermark);
        }
    }
    const auto lastForwarded = state.readU64();
    nautilus::invoke(absorbLastForwardedWatermark, session, lastForwarded);
    const auto slices = state.readU64();
    for (nautilus::val<uint64_t> sliceIndex = 0; sliceIndex < slices; ++sliceIndex)
    {
        const auto start = state.readU64();
        const auto end = state.readU64();
        nautilus::invoke(absorbSlice, session, start, end);
        const auto records = state.readU64();
        for (nautilus::val<uint64_t> record = 0; record < records; ++record)
        {
            const auto hash = state.readU64();
            const auto destination = nautilus::invoke(absorbEntry, session, sliceIndex, hash);
            state.readBytes(destination, payloadSize);
        }
    }
    const auto windows = state.readU64();
    for (nautilus::val<uint64_t> index = 0; index < windows; ++index)
    {
        const auto start = state.readU64();
        const auto end = state.readU64();
        const auto kind = state.readU64();
        nautilus::invoke(absorbWindow, session, start, end, kind);
    }
    nautilus::invoke(finishAggregationAbsorb, session);
}

void AggregationBuildPhysicalOperator::registerMigrationFunctions(CompilationContext& compilationContext) const
{
    for (const auto& function : aggregationPhysicalFunctions)
    {
        requireFixedAggregationState(*function);
    }
    const auto payloadSize = hashMapConfig.entrySize - sizeof(ChainedHashMapEntry);
    const std::function<void(nautilus::val<TupleBuffer*>, nautilus::val<const AggregationEmitSession*>, nautilus::val<uint64_t*>)>
        emitFunction
        = [this](
              nautilus::val<TupleBuffer*> output, nautilus::val<const AggregationEmitSession*> session, nautilus::val<uint64_t*> usedBytes)
    { traceEmitState(output, session, usedBytes); };
    compiledEmit = compilationContext.registerFunction(emitFunction, "emitAggregationState");

    const std::function<void(nautilus::val<const TupleBuffer*>, nautilus::val<AggregationAbsorbSession*>)> absorbFunction
        = [this](nautilus::val<const TupleBuffer*> input, nautilus::val<AggregationAbsorbSession*> session)
    { traceAbsorbState(input, session); };
    compiledAbsorb = compilationContext.registerFunction(absorbFunction, "absorbAggregationState");
}

AggregationEmitRuntime* AggregationBuildPhysicalOperator::beginEmit(
    const AggregationBuildPhysicalOperator* self, PipelineStateBuilder* state, PipelineExecutionContext* context)
{
    auto& handler = dynamic_cast<AggregationOperatorHandler&>(*context->getOperatorHandlers().at(self->operatorHandlerId));
    auto& store = dynamic_cast<DefaultTimeBasedSliceStore&>(handler.getSliceAndWindowStore());
    for (const auto& function : self->aggregationPhysicalFunctions)
    {
        requireFixedAggregationState(*function);
    }
    AggregationEmitSession session{
        .store = store.snapshot(), .watermarks = handler.snapshotWatermarkState(), .provider = context->getBufferManager()};
    const auto bytes = session.byteCount(self->hashMapConfig.entrySize - sizeof(ChainedHashMapEntry));
    auto buffer = context->getBufferManager()->getUnpooledBuffer(bytes);
    INVARIANT(buffer.has_value(), "Could not allocate window aggregation state buffer");
    return state->own(
        std::make_unique<AggregationEmitRuntime>(AggregationEmitRuntime{.session = std::move(session), .buffer = std::move(*buffer)}));
}

void AggregationBuildPhysicalOperator::finishEmit(PipelineStateBuilder* state, AggregationEmitRuntime* runtime)
{
    const auto bytes = runtime->buffer.getBufferSize();
    const auto usedBytes = runtime->usedBytes;
    INVARIANT(usedBytes <= bytes, "Compiled aggregation emit exceeded its buffer");
    if (usedBytes < bytes)
    {
        auto compact = runtime->session.provider->getUnpooledBuffer(usedBytes);
        INVARIANT(compact.has_value(), "Could not allocate compact aggregation state buffer");
        std::memcpy(compact->getAvailableMemoryArea().data(), runtime->buffer.getAvailableMemoryArea().data(), usedBytes);
        std::memset(compact->getAvailableMemoryArea().data() + usedBytes, 0, compact->getBufferSize() - usedBytes);
        state->addChild(std::move(*compact));
    }
    else
    {
        std::memset(runtime->buffer.getAvailableMemoryArea().data() + usedBytes, 0, bytes - usedBytes);
        state->addChild(std::move(runtime->buffer));
    }
}

AggregationAbsorbRuntime* AggregationBuildPhysicalOperator::beginAbsorb(
    const AggregationBuildPhysicalOperator* self, PipelineStateReader* state, PipelineExecutionContext* context)
{
    INVARIANT(state->childCount() == (self->getChild() ? 2 : 1), "Unexpected aggregation state children");
    auto& handler = dynamic_cast<AggregationOperatorHandler&>(*context->getOperatorHandlers().at(self->operatorHandlerId));
    auto& store = dynamic_cast<DefaultTimeBasedSliceStore&>(handler.getSliceAndWindowStore());
    return state->own(std::make_unique<AggregationAbsorbRuntime>(AggregationAbsorbRuntime{
        .buffer = state->child(0),
        .session = AggregationAbsorbSession{
            .handler = handler,
            .store = store,
            .config = self->hashMapConfig,
            .provider = context->getBufferManager(),
            .workerCount = context->getNumberOfWorkerThreads(),
            .slices = {},
            .windows = {},
            .watermarks = handler.snapshotWatermarkState(),
            .nextSequence = 0}}));
}

void AggregationBuildPhysicalOperator::lowerEmit(
    nautilus::val<PipelineStateBuilder*> state, nautilus::val<PipelineExecutionContext*> context) const
{
    const auto runtime = nautilus::invoke(beginEmit, nautilus::val<const AggregationBuildPhysicalOperator*>{this}, state, context);
    traceEmitState(
        nautilus::invoke(emitRuntimeBuffer, runtime),
        nautilus::invoke(emitRuntimeSession, runtime),
        nautilus::invoke(emitRuntimeUsedBytes, runtime));
    nautilus::invoke(finishEmit, state, runtime);
    PhysicalOperatorConcept::lowerEmit(state, context);
}

void AggregationBuildPhysicalOperator::lowerAbsorb(
    nautilus::val<PipelineStateReader*> state, nautilus::val<PipelineExecutionContext*> context) const
{
    if (not donorLayout)
    {
        nautilus::invoke(throwMissingAggregationDonor);
        return;
    }
    const auto runtime = nautilus::invoke(beginAbsorb, nautilus::val<const AggregationBuildPhysicalOperator*>{this}, state, context);
    traceAbsorbState(nautilus::invoke(absorbRuntimeBuffer, runtime), nautilus::invoke(absorbRuntimeSession, runtime));
    if (const auto child = getChild())
    {
        const auto childState = nautilus::invoke(aggregationChildReader, state);
        child->lowerAbsorb(childState, context);
        nautilus::invoke(finishAggregationChildReader, state, childState);
    }
    else
    {
        nautilus::invoke(finishAggregationLeafReader, state);
    }
}

void AggregationBuildPhysicalOperator::emitCompiled(PipelineStateBuilder& state, PipelineExecutionContext& context) const
{
    INVARIANT(compiledEmit.has_value(), "Aggregation emit function was not compiled");
    auto& handler = dynamic_cast<AggregationOperatorHandler&>(*context.getOperatorHandlers().at(operatorHandlerId));
    auto& store = dynamic_cast<DefaultTimeBasedSliceStore&>(handler.getSliceAndWindowStore());
    for (const auto& function : aggregationPhysicalFunctions)
    {
        requireFixedAggregationState(*function);
    }
    AggregationEmitSession session{
        .store = store.snapshot(), .watermarks = handler.snapshotWatermarkState(), .provider = context.getBufferManager()};
    const auto bytes = session.byteCount(hashMapConfig.entrySize - sizeof(ChainedHashMapEntry));
    auto buffer = context.getBufferManager()->getUnpooledBuffer(bytes);
    INVARIANT(buffer.has_value(), "Could not allocate window aggregation state buffer");
    uint64_t usedBytes = 0;
    (*compiledEmit)(std::addressof(*buffer), std::addressof(session), std::addressof(usedBytes));
    INVARIANT(usedBytes <= bytes, "Compiled aggregation emit exceeded its buffer");
    /// The first allocation is an upper bound based on the worker maps. Duplicate keys disappear during the merge.
    if (usedBytes < bytes)
    {
        auto compact = context.getBufferManager()->getUnpooledBuffer(usedBytes);
        INVARIANT(compact.has_value(), "Could not allocate compact aggregation state buffer");
        std::memcpy(compact->getAvailableMemoryArea().data(), buffer->getAvailableMemoryArea().data(), usedBytes);
        std::memset(compact->getAvailableMemoryArea().data() + usedBytes, 0, compact->getBufferSize() - usedBytes);
        state.addChild(std::move(*compact));
    }
    else
    {
        std::memset(buffer->getAvailableMemoryArea().data() + usedBytes, 0, buffer->getBufferSize() - usedBytes);
        state.addChild(std::move(*buffer));
    }
}

void AggregationBuildPhysicalOperator::absorbCompiled(PipelineStateReader& state, PipelineExecutionContext& context) const
{
    INVARIANT(donorLayout.has_value() && compiledAbsorb.has_value(), "Aggregation absorb function was not compiled with its donor");
    const auto child = getChild();
    INVARIANT(state.childCount() == (child ? 2 : 1), "Unexpected aggregation state children");
    auto buffer = state.child(0);
    auto& handler = dynamic_cast<AggregationOperatorHandler&>(*context.getOperatorHandlers().at(operatorHandlerId));
    auto& store = dynamic_cast<DefaultTimeBasedSliceStore&>(handler.getSliceAndWindowStore());
    AggregationAbsorbSession session{
        .handler = handler,
        .store = store,
        .config = hashMapConfig,
        .provider = context.getBufferManager(),
        .workerCount = context.getNumberOfWorkerThreads(),
        .slices = {},
        .windows = {},
        .watermarks = handler.snapshotWatermarkState(),
        .nextSequence = 0};
    (*compiledAbsorb)(std::addressof(buffer), std::addressof(session));
    if (child)
    {
        auto childBuffer = state.child(1);
        PipelineStateReader childState(childBuffer);
        child->absorb(childState, context);
    }
    state.ensureConsumed();
}
}
