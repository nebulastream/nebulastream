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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

enum class WertType : uint8_t
{
    PH,
    ABEc,
    NATRIUM,
    CHLORID,
    HCO3
};

struct AnionGapValue
{
    AnionGapValue(const int32_t patientId, const uint64_t timestamp, const double value, const std::string_view type)
    {
        this->patientId = patientId;
        this->timestamp = timestamp;
        this->value = value;
        if (type == "PH")
        {
            this->type = WertType::PH;
            return;
        }
        if (type == "ABEc")
        {
            this->type = WertType::ABEc;
            return;
        }
        if (type == "NATRIUM(BG)")
        {
            this->type = WertType::NATRIUM;
            return;
        }
        if (type == "CHLORID(BG)")
        {
            this->type = WertType::CHLORID;
            return;
        }
        if (type == "HCO3")
        {
            this->type = WertType::HCO3;
            return;
        }
        throw InvalidLiteral("Unexpected 'Wert' type in anion gap query: {}", type);
    }

    int32_t patientId{};
    uint64_t timestamp{};
    double value{std::numeric_limits<double>::quiet_NaN()};
    WertType type{};
};

class AnionGapTrigger
{
public:
    AnionGapTrigger() = default;

    explicit AnionGapTrigger(const AnionGapValue& gapVal) { setVal(gapVal.type, gapVal.timestamp, gapVal.value); };

    ~AnionGapTrigger() = default;

    std::pair<uint64_t, double> getPhVal() const { return std::make_pair(phTimestamp, phVal); }

    std::pair<uint64_t, double> getAbecVal() const { return std::make_pair(abecTimestamp, abecVal); }

    std::pair<uint64_t, double> getNatriumVal() const { return std::make_pair(natriumTimestamp, natriumVal); }

    std::pair<uint64_t, double> getChloridVal() const { return std::make_pair(chloridTimestamp, chloridVal); }

    std::pair<uint64_t, double> getHco3Val() const { return std::make_pair(hco3Timestamp, hco3Val); }

    std::optional<AnionGapTrigger> probe(const AnionGapValue& gapVal)
    {
        /// if all values exist and all timestamps are equal, apply formula
        /// HCO3: Bicarbonate
        /// ABEc: base excess
        /// NATRIUM(BG): Sodium
        /// PH: pH
        /// CHLORID(BG): Chloride
        ///
        /// High_anion_gap_metabolic_acidosis,"PH, ABEc ,NATRIUM(BG), CHLORID(BG), HCO3",
        /// if ( (pH<7.3) AND (base excess<-6) AND (Sodium - Chloride - Bicarbonate > 12) ) == TRUE
        /// "All input variables are from arterial blood gas sampled at the same time (labs[""Gruppe""]==""Blutgase arteriell""):"
        setVal(gapVal.type, gapVal.timestamp, gapVal.value);
        if (not(std::isnan(phVal)) and not(std::isnan(abecVal)) and not(std::isnan(natriumVal)) and not(std::isnan(chloridVal))
            and not(std::isnan(hco3Val)) and phTimestamp == abecTimestamp and phTimestamp == natriumTimestamp
            and phTimestamp == chloridTimestamp and phTimestamp == hco3Timestamp)
        {
            if ((phVal < 7.3) and (abecVal < -6.0) and (natriumVal - chloridVal - hco3Val > 12))
            {
                return std::optional{*this};
            }
        }
        return std::nullopt;
    }

private:
    uint64_t phTimestamp{};
    uint64_t abecTimestamp{};
    uint64_t natriumTimestamp{};
    uint64_t chloridTimestamp{};
    uint64_t hco3Timestamp{};
    double phVal{std::numeric_limits<double>::quiet_NaN()};
    double abecVal{std::numeric_limits<double>::quiet_NaN()};
    double natriumVal{std::numeric_limits<double>::quiet_NaN()};
    double chloridVal{std::numeric_limits<double>::quiet_NaN()};
    double hco3Val{std::numeric_limits<double>::quiet_NaN()};

    void setVal(const WertType type, const uint64_t timestamp, const double value)
    {
        switch (type)
        {
            case WertType::PH:
                this->phTimestamp = timestamp;
                this->phVal = value;
                break;
            case WertType::ABEc:
                this->abecTimestamp = timestamp;
                this->abecVal = value;
                break;
            case WertType::NATRIUM:
                this->natriumTimestamp = timestamp;
                this->natriumVal = value;
                break;
            case WertType::CHLORID:
                this->chloridTimestamp = timestamp;
                this->chloridVal = value;
                break;
            case WertType::HCO3:
                this->hco3Timestamp = timestamp;
                this->hco3Val = value;
                break;
                std::unreachable();
        }
    }
};

class StaticSharedDiffState
{
public:
    std::string probe(const AnionGapValue& newAnionGapValue)
    {
        std::scoped_lock lock(mutex);

        if (const auto patientBaseline = this->patienBaselinetMap.find(newAnionGapValue.patientId);
            patientBaseline != this->patienBaselinetMap.end())
        {
            if (const auto probeResultOpt = patientBaseline->second.probe(newAnionGapValue))
            {
                /// Probe succeeded, create alert
                const auto phVal = probeResultOpt.value().getPhVal();
                const auto abecVal = probeResultOpt.value().getAbecVal();
                const auto natriumVal = probeResultOpt.value().getNatriumVal();
                const auto chloridVal = probeResultOpt.value().getChloridVal();
                const auto hco3Val = probeResultOpt.value().getHco3Val();

                /// if ( (pH<7.3) AND (base excess<-6) AND (Sodium - Chloride - Bicarbonate > 12) ) == TRUE
                std::string alertMessage = std::format(
                    "High anion gap detected for patient {} at time: {}. Reason: if( ( PH({}) < 7.3) AND (Base Excess({}) < -6) AND (Sodium({}) - "
                    "Chloride({}) - Bicarbonate({}) > 12)).",
                    newAnionGapValue.patientId,
                    // std::chrono::sys_seconds{std::chrono::seconds(phVal.first)},
                    unixTsToFormattedDatetime(phVal.first),
                    phVal.second,
                    abecVal.second,
                    natriumVal.second,
                    chloridVal.second,
                    hco3Val.second);
                return alertMessage;
            }
            return "";
        }
        /// set baseline
        const auto newAnionGapTrigger = AnionGapTrigger{newAnionGapValue};
        patienBaselinetMap.emplace(std::make_pair(newAnionGapValue.patientId, newAnionGapTrigger));
        return "";
    }

private:
    std::unordered_map<uint64_t, AnionGapTrigger> patienBaselinetMap;
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

class HighAnionGapPhysicalFunction final
{
public:
    HighAnionGapPhysicalFunction(PhysicalFunction child);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const
    {
        /// get Wert as double
        const auto valueVal = child.execute(record, arena).cast<nautilus::val<double>>();
        auto patientIdVal = record.readUnqualified("MLIFE_FALLNR").cast<nautilus::val<int32_t>>();
        const auto bezeichnungVal = record.readUnqualified("BEZ").cast<VariableSizedData>();
        const auto zeitpunktVal = record.readUnqualified("ZEITPUNKT").cast<nautilus::val<uint64_t>>();
        nautilus::val<VarSizedResult*> probeResult = nautilus::invoke(
            +[](int32_t patientId, char* bez, size_t bezLength, double value, uint64_t timestamp, Arena* arenaPtr)
            {
                thread_local auto result = VarSizedResult{};
                const std::string_view bezSV{bez, bezLength};
                const std::string probeResult = staticSharedDiffState.probe(AnionGapValue{patientId, timestamp, value, bezSV});
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

private:
    PhysicalFunction child;
};

}