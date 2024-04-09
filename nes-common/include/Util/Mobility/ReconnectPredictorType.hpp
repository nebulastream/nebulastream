#ifndef NES_RECONNECTPREDICTORTYPE_HPP
#define NES_RECONNECTPREDICTORTYPE_HPP
#include <cstdint>

namespace NES::Spatial::Mobility::Experimental {

/**
 * @brief used in the mobility configuration to define the type of location provider to be constructed at the startup of a worker
 */
enum class ReconnectPredictorType : uint8_t {
    LIVE = 0,  //base class of location provider used for workers with a fixed location
    PRECALCULATED = 1,   //simulate location with coordinates read from csv
    INVALID = 2//the supplied configuration does not represent a valid provider type
};

}// namespace NES::Spatial::Mobility::Experimental
#endif//NES_RECONNECTPREDICTORTYPE_HPP
