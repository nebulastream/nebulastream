//
// Created by Toso, Lorenzo on 2019-01-23.
//

#pragma once

#include <iterator>

#define EACH_MEASURED_TIME MeasuredTimes::Value time = static_cast<MeasuredTimes::Value>(0); static_cast<int>(time) < MeasuredTimes::size();time = static_cast<MeasuredTimes::Value>(static_cast<int>(time)+1)

class MeasuredTimes {
private:
    static const int enum_length = 20;
public:
    enum Value {
        TOTAL,
        EFFECTIVE,
        SETUP,
        BUILD_PHASE,
        LOCAL_PROBING,
        REMOTE_PROBING,
        NETWORK_TRANSFER,
        LOCAL_RESULT_AGGREGATION,
        LOCAL_RESULT_AGGREGATION2,
        REMOTE_RESULT_AGGREGATION,
        SYNC,

        GPU_PROBE_COPY_DATA_TO_DEVICE,
        GPU_PROBE,
        GPU_COUNT_MATCHES,
        GPU_COPY_RESULTS_TO_HOST,
        GPU_PROBE_TOTAL,
        GPU_BUILD_TOTAL,
        GPU_BUILD_SETUP,
        GPU_BUILD,
        GPU_BUILD_COPY_DATA_TO_DEVICE,

    };
    static int size();
    static std::string to_string(Value v);



};



