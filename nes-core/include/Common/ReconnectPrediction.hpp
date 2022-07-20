
#ifndef NES_RECONNECTPREDICTION_HPP
#define NES_RECONNECTPREDICTION_HPP
#include <cstdint>
#include <Util/TimeMeasurement.hpp>

namespace NES::Spatial {

namespace Index::Experimental {
class Location;
}

namespace Mobility::Experimental {
struct ReconnectPrediction {
    uint64_t expectedNewParentId;
    Timestamp expectedTime;
};

struct ReconnectPoint {
    Index::Experimental::Location predictedReconnectLocation;
    ReconnectPrediction reconnectPrediction;
};
}
}

#endif//NES_RECONNECTPREDICTION_HPP
