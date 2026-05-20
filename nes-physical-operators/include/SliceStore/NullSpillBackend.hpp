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

#include <cstddef>
#include <optional>
#include <span>
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES
{

/// No-op SpillBackend: discards writes and never returns stored bytes.
///
/// Used as the default backend while spilling is disabled, and as a stand-in in
/// tests. Because nothing is persisted, `get` always reports a miss.
class NullSpillBackend final : public SpillBackend
{
public:
    void put(SliceEnd /*sliceEnd*/, WorkerThreadId /*workerThreadId*/, std::span<const std::byte> /*record*/) override { }

    std::optional<SpillRecord> get(SliceEnd /*sliceEnd*/, WorkerThreadId /*workerThreadId*/) override { return std::nullopt; }

    void erase(SliceEnd /*sliceEnd*/) override { }
};

}
