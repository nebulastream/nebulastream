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

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <Time/Timestamp.hpp>
#include <val_concepts.hpp>

namespace NES
{
class WindowBasedOperatorHandler;

/// Abstract interface for accessing operator-specific data structures from a SliceStore.
/// Each WindowSlicesStoreInterface implementation provides its own concrete SliceStoreRef.
/// The SliceStoreRef owns the SliceCache and encapsulates the cache + store interaction.
class SliceStoreRef
{
public:
    virtual ~SliceStoreRef() = default;

    /// Main entry point: looks up or creates the data structure for the given timestamp and worker thread.
    /// The extractor lambda converts an operator-agnostic Slice into the operator-specific data structure pointer.
    /// The createSlicesFunction is called on cache miss to get a fresh SliceCreateFunction.
    virtual nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const nautilus::val<OperatorHandler*>& operatorHandler)
        = 0;

    /// Necessary, as our PhysicalOperators get copied during the pipelining phase, but we need to ensure uniqueness for the slice store ref
    virtual std::unique_ptr<SliceStoreRef> clone() = 0;

    /// Initializes the slice store ref (e.g., allocates cache memory). Called during setup().
    virtual void setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx) = 0;
};

}
