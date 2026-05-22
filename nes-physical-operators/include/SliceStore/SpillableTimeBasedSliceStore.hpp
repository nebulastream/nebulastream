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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

/// Out-of-core variant of DefaultTimeBasedSliceStore (Phase 4b, Increment A — single-threaded spike).
///
/// It owns a SpillBackend and unspills any non-resident slice in the trigger read paths before returning
/// it (so the handler's triggerSlices always sees resident maps). Slices are spilled by an explicit,
/// test/operator-driven forceSpill(SliceEnd) — the automatic memory-budget cold scan and live eviction are
/// deferred to Increment B.
///
/// Spill target = COLD slices only (stopped filling, not yet emitted). A slice returned by a trigger feeder
/// is recorded in `emittedKeys`: its raw HashMap* is snapshotted for the asynchronous probe, so it must NOT
/// be spilled afterwards (async-probe-drain invariant) — forceSpill rejects it.
///
/// Implementation note: the base `windows`/`slices` maps are private, so this subclass deliberately uses only
/// the public WindowSlicesStoreInterface API plus two of its own tracked sets (`spilledKeys` for residency +
/// GC backend-erase, `emittedKeys` for the async-probe-drain guard). It reuses the inherited createSliceStoreRef
/// + SliceCache + DefaultTimeBasedSliceStoreRef unchanged (the build hot path only ever touches resident slices).
///
/// Not thread-safe beyond the base's per-map folly::Synchronized guarantees; multi-thread hardening is Increment B.
class SpillableTimeBasedSliceStore final : public DefaultTimeBasedSliceStore
{
public:
    SpillableTimeBasedSliceStore(
        uint64_t windowSize,
        uint64_t windowSlide,
        SliceCacheConfiguration sliceCacheConfiguration,
        std::unique_ptr<SpillBackend> backend,
        uint64_t entriesPerPage,
        uint64_t pageSize,
        uint64_t softThresholdBytes,
        uint64_t hardThresholdBytes);

    /// Builds a durable RocksDB-backed SpillBackend. Defined in this translation unit so RocksDB stays a PRIVATE
    /// dependency of nes-physical-operators — the lowering rule (nes-query-compiler) passes only SpillConfig values
    /// and never links rocksdb.
    static std::unique_ptr<SpillBackend> makeRocksDBBackend(const std::string& rocksdbPath, const std::string& compression);

    /// D3 unspill-on-read: every slice returned is unspilled (if non-resident) and recorded as emitted (its raw
    /// HashMap* will be snapshotted for the async probe → it must not be spilled afterwards).
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    /// Random-access read (join-probe path; out of 4b scope): unspills for consistency, but does NOT mark emitted.
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;
    /// Erases backend records for spilled slices that the base GC has dropped.
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    /// Erases all backend records + clears tracked sets before delegating, so an explicit deleteState() (not just
    /// destruction) leaves no orphaned on-disk records or stale residency bookkeeping.
    void deleteState() override;

    /// O1: stash the query-lifetime buffer provider used to rebuild spilled maps on unspill. Set once
    /// (called from setupSliceStoreProxy); the store unit test calls it directly.
    void setBufferProvider(std::shared_ptr<AbstractBufferProvider> provider);

    /// Increment A forced/manual spill driver: spill a provably COLD slice (the caller selects it; automatic
    /// cold detection is Increment B). No-op if the slice is unknown or already spilled. Throws SpillStoreFailure
    /// if the slice has already been emitted to the probe (async-probe-drain invariant).
    void forceSpill(SliceEnd sliceEnd);

    /// True unless the slice is currently spilled. Unknown slices report resident.
    [[nodiscard]] bool isSliceResident(SliceEnd sliceEnd) const;

    /// Resident memory footprint of one slice = Σ_workers ⌈tuples_w / entriesPerPage⌉ × pageSize. Returns 0 for a
    /// spilled slice (its maps are freed). Exact for fixed-size entries.
    [[nodiscard]] uint64_t residentBytesOf(const Slice& slice) const;

private:
    /// For each returned slice: unspill if non-resident (dropping it from spilledKeys) and record it emitted.
    void unspillAndMarkEmitted(std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& windowSlices);

    std::unique_ptr<SpillBackend> backend;
    folly::Synchronized<std::shared_ptr<AbstractBufferProvider>> bufferProvider;
    const uint64_t entriesPerPage;
    const uint64_t pageSize;
    /// Recorded now (plumbed from SpillConfig) but not yet enforced: Increment A spills only via forceSpill.
    /// Automatic soft/hard-threshold-driven eviction is Increment B (O4). [[maybe_unused]] until then.
    [[maybe_unused]] const uint64_t softThresholdBytes;
    [[maybe_unused]] const uint64_t hardThresholdBytes;
    /// SliceEnds whose maps are currently spilled — drives isSliceResident() and GC backend-erase.
    folly::Synchronized<std::set<SliceEnd>> spilledKeys;
    /// SliceEnds returned by a trigger feeder (emitted to the async probe) — forceSpill refuses these.
    folly::Synchronized<std::set<SliceEnd>> emittedKeys;
};

}
