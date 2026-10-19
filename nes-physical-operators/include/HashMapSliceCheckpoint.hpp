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

#include <filesystem>
#include <functional>
#include <memory>
#include <vector>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{
class WindowBasedOperatorHandler;

/// Writes the HashMapSlices of a window-based operator handler, e.g., of an aggregation or a hash join, into its checkpoint state
/// directory and restores them into a fresh handler.
class HashMapSliceCheckpoint
{
public:
    /// Creates slices exactly like the build operators do, so that restored slices are indistinguishable from slices created by the build.
    using CreateSlicesFunction = std::function<std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>(
        WindowBasedOperatorHandler&, AbstractBufferProvider&)>;

    HashMapSliceCheckpoint(std::vector<ChainedHashMapConfig> configPerInputStream, CreateSlicesFunction createSlices);

    /// The caller has to hold the handler's checkpoint write lock.
    void write(WindowBasedOperatorHandler& handler, const std::filesystem::path& stateDirectory) const;

    /// Fails with a CheckpointError if a checkpoint exists but cannot be restored. Restarting from empty state instead would silently
    /// lose the input that the sources already discarded because the checkpoint covered it.
    void restore(
        WindowBasedOperatorHandler& handler,
        const std::filesystem::path& recoveryStateDirectory,
        AbstractBufferProvider& bufferProvider) const;

private:
    std::vector<ChainedHashMapConfig> configPerInputStream;
    CreateSlicesFunction createSlices;
};

}
