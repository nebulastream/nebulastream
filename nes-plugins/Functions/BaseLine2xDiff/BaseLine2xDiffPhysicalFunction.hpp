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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

struct BaselineValues
{
    int32_t patientId;
    uint64_t timestamp;
    double value;
};

class StaticSharedDiffState
{
public:
    std::string probe(const BaselineValues& newBaselineValue)
    {
        std::scoped_lock lock(mutex);

        if (const auto patientBaseline = this->patienBaselinetMap.find(newBaselineValue.patientId);
            patientBaseline != this->patienBaselinetMap.end())
        {
            if (patientBaseline->second.value * 2 <= newBaselineValue.value)
            {
                /// Acute_kidney_injury_creatinine_criterion,"KREATININ, Zeitpunkt",( ( Serum_creatinine(t1) >= 2*Serum_creatinine(t0) ) == TRUE ) AND ( ( t1 < t0 + 7 days ) == TRUE ) ) == TRUE,
                /// "Serum_creatinine(t0): chronologically first serum creatinine value recorded per hospital stay (-> per mlife_ANR), Serum_creatinine(t1):
                /// most recently recorded serum creatinine value. The second condition, regarding the time delta between measurement points
                /// (i.e., the serum creatinine increase has to happen within the first 7 days to be classified as acute renal injury), would be nice to have but is not absolutely necessary.
                /// In other words, the creatinine surge could also happen beyond the first 7 days but this would be less correct. To be on the safer side, specificy ""Gruppe"" for this one --> labs[""Gruppe""] == ""Retention"""
                std::string alertMessage
                    = fmt::format(
                          "Acute kidney injury detected for patient {} at time: {}. Reason: serum_Kreatinin value of {} is (more than) twice as large as baseline value of {} measured at {}.",
                          newBaselineValue.patientId,
                          unixTsToFormattedDatetime(newBaselineValue.timestamp),
                          newBaselineValue.value,
                          patientBaseline->second.value,
                          unixTsToFormattedDatetime(patientBaseline->second.timestamp));
                constexpr size_t sevenDaysInMilliseconds = 604800000;
                if (newBaselineValue.timestamp - patientBaseline->second.timestamp >= sevenDaysInMilliseconds)
                {
                    alertMessage.append(" Note: Values were measure more than one week apart.");
                }
                return alertMessage;
            }
            return "";
        }
        /// set baseline
        patienBaselinetMap.emplace(std::make_pair(newBaselineValue.patientId, newBaselineValue));
        return "";
    }

private:
    std::unordered_map<uint64_t, BaselineValues> patienBaselinetMap;
    std::mutex mutex{};

    std::string unixTsToFormattedDatetime(const uint64_t unixTimestamp)
    {
        auto tp = std::chrono::sys_seconds{std::chrono::seconds(unixTimestamp)};
        auto dp = std::chrono::floor<std::chrono::days>(tp);
        std::chrono::year_month_day ymd{dp};
        std::chrono::hh_mm_ss hms{tp - dp};

        std::ostringstream oss;
        oss << std::setfill('0')
            << std::setw(2) << static_cast<unsigned>(ymd.day())   << "."
            << std::setw(2) << static_cast<unsigned>(ymd.month()) << "."
            << static_cast<int>(ymd.year())                        << " "
            << std::setw(2) << hms.hours().count()                 << ":"
            << std::setw(2) << hms.minutes().count();

        return oss.str();
    }
};

static StaticSharedDiffState staticSharedDiffState;

struct VarSizedResult
{
    const char* varSizedPointer;
    uint64_t size{};
};

class BaseLine2xDiffPhysicalFunction final
{
public:
    BaseLine2xDiffPhysicalFunction(PhysicalFunction child);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const
    {
        // [x] Todo: determine if it is possible to get all fields from the record
        //      - looks good
        if (record.hasField("ODBC_SOURCE$WERT"))
        {
            /// get Wert as double
            const auto valueVal = child.execute(record, arena).cast<nautilus::val<double>>();
            auto patientIdVal = record.read("ODBC_SOURCE$MLIFE_FALLNR").cast<nautilus::val<int32_t>>();
            const auto bezeichnungVal = record.read("ODBC_SOURCE$BEZ").cast<VariableSizedData>();
            const auto zeitpunktVal = record.read("ODBC_SOURCE$ZEITPUNKT").cast<nautilus::val<uint64_t>>();
            nautilus::val<VarSizedResult*> probeResult = nautilus::invoke(
                +[](int32_t patientId, char* bez, size_t bezLength, double value, uint64_t timestamp, Arena* arenaPtr)
                {
                    thread_local auto result = VarSizedResult{};
                    const std::string_view bezSV{bez, bezLength};
                    // Todo: can remove check
                    INVARIANT(bezSV == "Kreatinin", "Function only works for Kreatinin values, apply WHERE filter first");
                    const std::string probeResult
                        = staticSharedDiffState.probe(BaselineValues{.patientId = patientId, .timestamp = timestamp, .value = value});
                    const auto allocatedMemory = reinterpret_cast<char*>(arenaPtr->allocateMemory(probeResult.size()).data());
                    std::memcpy(allocatedMemory, probeResult.data(), probeResult.size());
                    result = VarSizedResult{.varSizedPointer = allocatedMemory, .size = probeResult.size()};
                    return &result;
                },
                patientIdVal,
                bezeichnungVal.getContent(),
                bezeichnungVal.getSize(),
                valueVal,
                zeitpunktVal,
                arena.getArena());
            VariableSizedData varSizedString{
                *getMemberWithOffset<int8_t*>(probeResult, offsetof(VarSizedResult, varSizedPointer)),
                *getMemberWithOffset<uint64_t>(probeResult, offsetof(VarSizedResult, size))};
            return varSizedString;
        }
        throw FieldNotFound("BaseLine2xDiffPhysicalFunction require 'Wert' field");
    }

private:
    PhysicalFunction child;
};

}