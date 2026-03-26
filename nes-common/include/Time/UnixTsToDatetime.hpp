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
#include <compare>
#include <cstdint>
#include <ostream>
#include <chrono>

namespace NES
{

inline std::string unixTsToFormattedDatetimeAdjusted(const uint64_t unixTimestamp)
{
    const auto rawTime = static_cast<time_t>(unixTimestamp);
    std::tm localTm{};
    localtime_r(&rawTime, &localTm);

    return fmt::format(
        "{:02d}.{:02d}.{} {:02d}:{:02d}:{:02d}({})",
        localTm.tm_mday,
        localTm.tm_mon + 1,
        localTm.tm_year + 1900,
        localTm.tm_hour,
        localTm.tm_min,
        localTm.tm_sec,
        unixTimestamp);
}
inline std::string unixTsToFormattedDatetime(const uint64_t unixTimestamp)
{
    auto tp = std::chrono::sys_seconds{std::chrono::seconds(unixTimestamp)};
    auto dp = std::chrono::floor<std::chrono::days>(tp);
    const std::chrono::year_month_day ymd{dp};
    const std::chrono::hh_mm_ss hms{tp - dp};

    return fmt::format(
        "{:02d}.{:02d}.{} {:02d}:{:02d}:{:02d}({})",
        static_cast<unsigned>(ymd.day()),
        static_cast<unsigned>(ymd.month()),
        static_cast<int>(ymd.year()),
        hms.hours().count(),
        hms.minutes().count(),
        hms.seconds().count(),
        unixTimestamp);
}

}

