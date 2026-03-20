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

#include <chrono>
#include <string>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Time/UnixTsToDatetime.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

struct VarSizedResult
{
    const char* varSizedPointer;
    uint64_t size{};
};

class UnixTimestampToDatetimePhysicalFunction final
{
public:
    UnixTimestampToDatetimePhysicalFunction(PhysicalFunction child);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const
    {
        auto patientIdVal = record.readUnqualified("MLIFE_PID").cast<nautilus::val<int32_t>>();
        const auto zeitpunktVal = record.readUnqualified("ZEITPUNKT").cast<nautilus::val<uint64_t>>();
        const auto insertionTs = record.readUnqualified("TSFAIL").cast<nautilus::val<uint64_t>>();
        const auto ingestionTs = record.readUnqualified("TSLOG").cast<nautilus::val<uint64_t>>();
        nautilus::val<VarSizedResult*> probeResult = nautilus::invoke(
            +[](const int32_t patientId, uint64_t unixTimestamp, const uint64_t insertionTs, const uint64_t ingestionTs, Arena* arenaPtr)
            {
                thread_local auto result = VarSizedResult{};

                const auto currentTs
                    = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                const auto probeResult = fmt::format(
                    "{}. Timestamps(Creation: {}, MLIFE(Clone) Insertion: {}, System Ingestion: {}, Alarm: {})",
                    patientId,
                    unixTsToFormattedDatetime(unixTimestamp),
                    unixTsToFormattedDatetime(insertionTs),
                    unixTsToFormattedDatetime(ingestionTs),
                    unixTsToFormattedDatetime(currentTs));

                const auto allocatedMemory = reinterpret_cast<char*>(arenaPtr->allocateMemory(probeResult.size()).data());
                std::memcpy(allocatedMemory, probeResult.data(), probeResult.size());
                result = VarSizedResult{.varSizedPointer = allocatedMemory, .size = probeResult.size()};
                return &result;
            },
            patientIdVal,
            zeitpunktVal,
            insertionTs,
            ingestionTs,
            arena.getArena());
        VariableSizedData varSizedString{
            *getMemberWithOffset<int8_t*>(probeResult, offsetof(VarSizedResult, varSizedPointer)),
            *getMemberWithOffset<uint64_t>(probeResult, offsetof(VarSizedResult, size))};
        return varSizedString;
    }

private:
    PhysicalFunction child;
};
}