
#include "TimeTools.hpp"
#include <algorithm>
#include "MathUtils.h"

Timestamp TimeTools::now() {
    return static_cast<Timestamp>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now().time_since_epoch())
                .count());
}

TimeTools::InterestingTimes TimeTools::get_interesting_times(const std::vector<Timestamp> &times) {
    TimeTools::InterestingTimes interesting_times{};

    interesting_times.time_count = times.size();
    interesting_times.min_time = *std::min_element(times.begin(), times.end());
    interesting_times.max_time = *std::max_element(times.begin(), times.end());
    interesting_times.mean_time = std::accumulate(times.begin(), times.end(), (size_t)0)/times.size();
    interesting_times.median_time = Math::median(times.begin(), times.end());
    interesting_times.std = Math::std(times.begin(), times.end());

    return interesting_times;
}

Timestamp TimeTools::millis() {
    return (TimeTools::now()/10000000)%10000;
}

Json TimeTools::InterestingTimes::to_json() {
    Json::object j;
    j["time_count"] = (int)time_count;
    j["min_time"] = (double)min_time;
    j["max_time"] = (double)max_time;
    j["mean_time"] = (double)mean_time;
    j["median_time"] = (double)median_time;
    j["std"] = std;
    return j;
}

void TimeTools::InterestingTimes::print(int divider, const std::string &unit)
    {
        printf("Iterations: %lu\n", time_count);
        printf("min_time: %lu%s\n", min_time/divider, unit.c_str());
        printf("max_time: %lu%s\n", max_time/divider, unit.c_str());
        printf("mean_time: %lu%s\n", mean_time/divider, unit.c_str());
        printf("median_time: %lu%s\n", median_time/divider, unit.c_str());
        printf("stdev: %.5f%s\n", std/divider, unit.c_str());
    }
