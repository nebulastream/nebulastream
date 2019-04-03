#ifndef MAMPI_TIMETOOLS_H
#define MAMPI_TIMETOOLS_H

#include <chrono>
#include <cstdint>
#include <vector>
#include <cstdio>
#include <string>
#include <json11/json11.hpp>

using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;
typedef uint64_t Timestamp;
using namespace json11;

class TimeTools {
public:
    struct InterestingTimes {
        size_t time_count;
        size_t min_time;
        size_t max_time;
        size_t mean_time;
        size_t median_time;
        double std;

        void print(int divider, const std::string & unit);
        Json to_json();
    };

    static Timestamp now();
    static Timestamp millis();
    static InterestingTimes get_interesting_times(const std::vector<Timestamp> & times);



};

#endif
