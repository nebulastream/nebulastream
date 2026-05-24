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

#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ScalarOption.hpp>

namespace NES
{

/// Configuration for the out-of-core (spillable) window-slice store.
///
/// Phase 1 (skeleton) only declares the knobs and wires them into
/// QueryExecutionConfiguration; nothing spills yet. When `enabled` is false
/// (the default) the lowering keeps using the in-memory DefaultTimeBasedSliceStore.
/// The remaining fields are consumed by later phases (RocksDB backend, memory
/// governor) and are validated there, so this config stays header-only and
/// dependency-light, mirroring SliceCacheConfiguration.
class SpillConfig final : public BaseConfiguration
{
public:
    SpillConfig() = default;
    SpillConfig(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { }

    /// Master switch. While false, the slice store is fully in-memory.
    BoolOption enabled = {"enabled", "false", "Enable the spillable (out-of-core) window-slice store."};

    /// Directory backing the RocksDB instance used for spilled slices (Phase 3+).
    /// The /tmp default is a development placeholder; deployments should set a durable path.
    StringOption rocksdbPath = {"rocksdb_path", "/tmp/nes-spill", "Filesystem path for the RocksDB spill backend (set a durable path in production)."};

    /// Soft memory watermark (MB). Crossing it triggers background spilling (Phase 5).
    UIntOption softThresholdMB = {"soft_threshold_mb", "256", "Soft memory watermark in MB; background spill above this."};

    /// Hard memory watermark (MB). Crossing it triggers synchronous spilling (Phase 5).
    UIntOption hardThresholdMB = {"hard_threshold_mb", "384", "Hard memory watermark in MB; synchronous spill above this."};

    /// RocksDB block compression for spilled slices (Phase 3+).
    StringOption compression = {"compression", "lz4", "Compression for spilled slices [lz4|zstd|none]."};

    /// RocksDB write-buffer (memtable) size in MB (Phase 0a). RocksDB's untuned default (~64 MB per instance) is a
    /// fixed memory tax under a tight cap (see E0). 0 = leave RocksDB's own default (behavior-preserving).
    UIntOption writeBufferSizeMB
        = {"write_buffer_size_mb", "0", "RocksDB write-buffer (memtable) size in MB for spilled slices; 0 = RocksDB default."};

    /// RocksDB block-cache size in MB (Phase 0a). Bounds the read-cache footprint of the spill backend.
    /// 0 = leave RocksDB's own default (behavior-preserving).
    UIntOption blockCacheSizeMB
        = {"block_cache_size_mb", "0", "RocksDB block-cache size in MB for the spill backend; 0 = RocksDB default."};

    /// Event-time emit/retention lag L (same unit as the window/watermark, ms). Results emit on a watermark of
    /// globalWatermark - emit_lag, so completed windows in [globalWatermark - L, globalWatermark) are RETAINED and
    /// become spillable (Increment C, emit-decouple). 0 = today's behavior (emit wm == global wm).
    UIntOption emitLag
        = {"emit_lag", "0", "Event-time emit/retention lag L (ms); retain+spill completed windows behind the watermark. 0 = no lag."};

    /// Number of grace-hash partitions for spilled window state at terminal emit (Phase 2, 2d). 1 (default) bypasses
    /// partitioning entirely and is byte-identical to the non-partitioned emit; P>1 splits each spilled worker map by
    /// `hash % P` so each emitted chunk materialises only 1/P of the window state at a time (textbook grace-hash).
    UIntOption numberOfPartitions
        = {"number_of_partitions",
           "1",
           "Number of grace-hash partitions for spilled window state at terminal emit; 1 (default) = no partitioning "
           "(byte-identical to non-partitioned emit)."};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &enabled,
            &rocksdbPath,
            &softThresholdMB,
            &hardThresholdMB,
            &compression,
            &writeBufferSizeMB,
            &blockCacheSizeMB,
            &emitLag,
            &numberOfPartitions};
    }
};

}
