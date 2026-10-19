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

#include <HashMapSliceCheckpoint.hpp>

#include <array>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
namespace
{
constexpr std::string_view CheckpointFileName = "hash_map_slices.bin";
constexpr std::array<char, 8> CheckpointMagic{'N', 'E', 'S', 'H', 'M', 'S', '0', '2'};

template <typename T>
void writeValue(std::ostream& out, const T& value)
{
    out.write(reinterpret_cast<const char*>(&value), sizeof(T)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

template <typename T>
T readValue(std::istream& in)
{
    T value{};
    in.read(reinterpret_cast<char*>(&value), sizeof(T)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!in)
    {
        throw CannotDeserialize("Truncated hash map slice checkpoint");
    }
    return value;
}

HashMapSlice& asHashMapSlice(const std::shared_ptr<Slice>& slice)
{
    auto* hashMapSlice = dynamic_cast<HashMapSlice*>(slice.get());
    INVARIANT(hashMapSlice != nullptr, "Hash map slice checkpoints only support HashMapSlices");
    return *hashMapSlice;
}
}

HashMapSliceCheckpoint::HashMapSliceCheckpoint(std::vector<ChainedHashMapConfig> configPerInputStream, CreateSlicesFunction createSlices)
    : configPerInputStream(std::move(configPerInputStream)), createSlices(std::move(createSlices))
{
}

void HashMapSliceCheckpoint::write(WindowBasedOperatorHandler& handler, const std::filesystem::path& stateDirectory) const
{
    auto& sliceStore = handler.getSliceAndWindowStore();
    const auto slices = sliceStore.getActiveSlices();
    const auto emittedWindowsEnd = sliceStore.getEmittedWindowsEnd();
    const auto checkpointFile = stateDirectory / CheckpointFileName;
    const auto temporaryCheckpointFile = std::filesystem::path(checkpointFile.string() + ".tmp");
    std::filesystem::create_directories(stateDirectory);
    {
        std::ofstream out(temporaryCheckpointFile, std::ios::binary | std::ios::trunc);
        writeValue(out, CheckpointMagic);
        writeValue(out, emittedWindowsEnd.getRawValue());
        writeValue(out, static_cast<uint64_t>(slices.size()));
        for (const auto& slice : slices)
        {
            writeValue(out, slice->getSliceStart().getRawValue());
            writeValue(out, slice->getSliceEnd().getRawValue());
            asHashMapSlice(slice).serializeHashMaps(out, configPerInputStream);
        }
        out.close();
        if (!out)
        {
            throw CheckpointError("Cannot write hash map slice checkpoint {}", temporaryCheckpointFile.string());
        }
    }
    std::error_code errorCode;
    std::filesystem::rename(temporaryCheckpointFile, checkpointFile, errorCode);
    if (errorCode)
    {
        throw CheckpointError("Cannot replace hash map slice checkpoint {}: {}", checkpointFile.string(), errorCode.message());
    }
    NES_DEBUG("Checkpointed {} slices to {}", slices.size(), checkpointFile.string());
}

void HashMapSliceCheckpoint::restore(
    WindowBasedOperatorHandler& handler, const std::filesystem::path& recoveryStateDirectory, AbstractBufferProvider& bufferProvider) const
{
    const auto checkpointFile = recoveryStateDirectory / CheckpointFileName;
    if (!std::filesystem::exists(checkpointFile))
    {
        return;
    }

    auto& sliceStore = handler.getSliceAndWindowStore();
    try
    {
        std::ifstream in(checkpointFile, std::ios::binary);
        if (readValue<std::array<char, CheckpointMagic.size()>>(in) != CheckpointMagic)
        {
            throw CannotDeserialize("Unknown checkpoint format");
        }
        const auto createSlice = createSlices(handler, bufferProvider);
        const Timestamp emittedWindowsEnd{readValue<uint64_t>(in)};
        const auto numberOfSlices = readValue<uint64_t>(in);
        for (uint64_t sliceIndex = 0; sliceIndex < numberOfSlices; ++sliceIndex)
        {
            const SliceStart sliceStart{readValue<uint64_t>(in)};
            const SliceEnd sliceEnd{readValue<uint64_t>(in)};
            const auto slices = sliceStore.getSlicesOrCreate(
                sliceStart,
                [&](const SliceStart assignedSliceStart, const SliceEnd assignedSliceEnd)
                {
                    if (assignedSliceStart != sliceStart || assignedSliceEnd != sliceEnd)
                    {
                        throw CannotDeserialize(
                            "Slice {}-{} does not match the window definition, which assigns slice {}-{}",
                            sliceStart,
                            sliceEnd,
                            assignedSliceStart,
                            assignedSliceEnd);
                    }
                    return createSlice(assignedSliceStart, assignedSliceEnd);
                });
            INVARIANT(slices.size() == 1, "Expected exactly one slice for slice start {}, but got {}", sliceStart, slices.size());
            asHashMapSlice(slices.front()).deserializeHashMaps(in, configPerInputStream, bufferProvider);
        }
        sliceStore.markWindowsAsEmitted(emittedWindowsEnd);
        NES_INFO("Restored {} slices from {}", numberOfSlices, checkpointFile.string());
    }
    catch (const std::exception& exception)
    {
        sliceStore.deleteState();
        throw CheckpointError("Cannot restore the window state checkpointed in {}: {}", checkpointFile.string(), exception.what());
    }
}

}
