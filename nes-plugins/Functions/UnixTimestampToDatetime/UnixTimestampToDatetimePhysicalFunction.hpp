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
        auto patientIdVal = record.readUnqualified("MLIFE_FALLNR").cast<nautilus::val<int32_t>>();
        const auto zeitpunktVal = record.readUnqualified("ZEITPUNKT").cast<nautilus::val<uint64_t>>();
        nautilus::val<VarSizedResult*> probeResult = nautilus::invoke(
            +[](const int32_t patientId, uint64_t unixTimestamp, Arena* arenaPtr)
            {
                thread_local auto result = VarSizedResult{};

                auto tp = std::chrono::sys_seconds{std::chrono::seconds(unixTimestamp)};
                auto dp = std::chrono::floor<std::chrono::days>(tp);
                const std::chrono::year_month_day ymd{dp};
                const std::chrono::hh_mm_ss hms{tp - dp};
                const std::string probeResult = fmt::format(
                    "{} at {:02d}.{:02d}.{} {:02d}:{:02d}({})",
                    patientId,
                    static_cast<unsigned>(ymd.day()),
                    static_cast<unsigned>(ymd.month()),
                    static_cast<int>(ymd.year()),
                    hms.hours().count(),
                    hms.minutes().count(),
                    unixTimestamp);

                const auto allocatedMemory = reinterpret_cast<char*>(arenaPtr->allocateMemory(probeResult.size()).data());
                std::memcpy(allocatedMemory, probeResult.data(), probeResult.size());
                result = VarSizedResult{.varSizedPointer = allocatedMemory, .size = probeResult.size()};
                return &result;
            },
            patientIdVal,
            zeitpunktVal,
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