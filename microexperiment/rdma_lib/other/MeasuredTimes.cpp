//
// Created by Toso, Lorenzo on 2019-01-23.
//

#include "MeasuredTimes.h"


int MeasuredTimes::size() {
    return enum_length;
}

std::string MeasuredTimes::to_string(MeasuredTimes::Value v) {
    switch(v){
        case TOTAL: return "TOTAL";
        case EFFECTIVE: return "EFFECTIVE";
        case BUILD_PHASE: return "BUILD_PHASE";
        case LOCAL_PROBING: return "LOCAL_PROBING";
        case REMOTE_PROBING: return "REMOTE_PROBING";
        case NETWORK_TRANSFER: return "NETWORK_TRANSFER";
        case SETUP: return "SETUP";
        case LOCAL_RESULT_AGGREGATION: return "LOCAL_RESULT_AGGREGATION";
        case LOCAL_RESULT_AGGREGATION2: return "LOCAL_RESULT_AGGREGATION2";
        case REMOTE_RESULT_AGGREGATION: return "REMOTE_RESULT_AGGREGATION";
        case SYNC: return "SYNC";
        case GPU_PROBE_COPY_DATA_TO_DEVICE: return "GPU_PROBE_COPY_DATA_TO_DEVICE";
        case GPU_PROBE: return "GPU_PROBE";
        case GPU_COUNT_MATCHES: return "GPU_COUNT_MATCHES";
        case GPU_COPY_RESULTS_TO_HOST: return "GPU_COPY_RESULTS_TO_HOST";
        case GPU_PROBE_TOTAL: return "GPU_PROBE_TOTAL";
        case GPU_BUILD_TOTAL:
            return "GPU_BUILD_TOTAL";
        case GPU_BUILD_COPY_DATA_TO_DEVICE:
            return "GPU_BUILD_COPY_DATA_TO_DEVICE";
        case GPU_BUILD:
            return "GPU_BUILD";
        case GPU_BUILD_SETUP:
            return "GPU_BUILD_SETUP";
        default:
            return "FORGOT TO ADD TO_STRING!";
    }
}
