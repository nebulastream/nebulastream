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

// class StaticSharedDiffState
// {
// public:
//     bool probe(int32_t patientId, const std::string_view& bezeichnung, const std::string_view& gruppe, double value, uint64_t timestamp)
//     {
//         std::scoped_lock lock(mutex);
//         std::string valueString{bezeichnung};
//         valueString.append(gruppe);
//
//         // if (const auto prior = this->baselineMap.find(std::make_pair(patientId, valueString)); prior != this->baselineMap.end())
//         if (const auto patientMap = this->baselineMap.find(patientId); prior != this->baselineMap.end())
//         {
//             if (const auto prior = patientMap->second.find(valueString); prior != patientMap->second.end())
//             {
//                 if (prior->second.value * 2 >= value)
//                 {
//                     return true;
//                 }
//                 return false;
//             }
//             /// set baseline
//             patientMap->second.emplace(
//                 std::make_pair(valueString, BaselineValues{.patientId = patientId, .timestamp = timestamp, .value = value}));
//             return false;
//         }
//         /// set new patient map (with first baseline value)
//         std::unordered_map<std::string, BaselineValues> newPatientMap;
//         newPatientMap.emplace(std::make_pair(valueString, BaselineValues{.patientId = patientId, .timestamp = timestamp, .value = value}));
//         this->baselineMap.emplace(std::make_pair(patientId, newPatientMap));
//         return false;
//     }
//
// private:
//     std::unordered_map<uint64_t, std::unordered_map<std::string, BaselineValues>> baselineMap;
//     std::mutex mutex{};
// };

struct BaselineValues
{
    int32_t patientId;
    uint64_t timestamp;
    double value;
};

class StaticSharedDiffState
{
public:
    bool probe(int32_t patientId, const std::string_view& bezeichnung, const std::string_view& gruppe, double value, uint64_t timestamp)
    {
        std::scoped_lock lock(mutex);
        std::string valueString{bezeichnung};
        valueString.append(gruppe);

        // if (const auto prior = this->baselineMap.find(std::make_pair(patientId, valueString)); prior != this->baselineMap.end())
        if (const auto patientBaseline = this->patienBaselinetMap.find(patientId); patientBaseline != this->patienBaselinetMap.end())
        {
            if (patientBaseline->second.value * 2 <= value)
            {
                return true;
            }
            return false;
        }
        /// set baseline
        patienBaselinetMap.emplace(
            std::make_pair(patientId, BaselineValues{.patientId = patientId, .timestamp = timestamp, .value = value}));
        return false;
    }

private:
    std::unordered_map<uint64_t, BaselineValues> patienBaselinetMap;
    std::mutex mutex{};
};

static StaticSharedDiffState staticSharedDiffState;

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
            const auto gruppeVal = record.read("ODBC_SOURCE$GRUPPE").cast<VariableSizedData>();
            const auto zeitpunktVal = record.read("ODBC_SOURCE$ZEITPUNKT").cast<nautilus::val<int32_t>>();
            nautilus::val<bool> probeResult = nautilus::invoke(
                +[](int32_t patientId, char* bez, size_t bezLength, char* gruppe, size_t gruppeLength, double value, uint64_t timestamp)
                {
                    const std::string_view bezSV{bez, bezLength};
                    if (bezSV != "Kreatinin")
                    {
                        return false;
                    }
                    const std::string_view gruppeSV{gruppe, gruppeLength};
                    return staticSharedDiffState.probe(patientId, bezSV, gruppeSV, value, timestamp);
                },
                patientIdVal,
                bezeichnungVal.getContent(),
                bezeichnungVal.getSize(),
                gruppeVal.getContent(),
                gruppeVal.getSize(),
                valueVal,
                zeitpunktVal);
            return probeResult;
        }
        throw FieldNotFound("BaseLine2xDiffPhysicalFunction require 'Wert' field");
        // auto variableSized = arena.allocateVariableSizedData(2);
        // *variableSized.getContent() = 'o';
        // *variableSized.getContent() +1 = 'k';
        // const auto leftValue = child.execute(record, arena).cast<nautilus::val<double>>();
        // return variableSized;
    }

private:
    PhysicalFunction child;
};

}