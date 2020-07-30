#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#include <cstdint>

class NetworkValues {
  public:
    NetworkValues() = default;

    uint64_t rBytes;
    uint64_t rPackets;
    uint64_t rErrs;
    uint64_t rDrop;
    uint64_t rFifo;
    uint64_t rFrame;
    uint64_t rCompressed;
    uint64_t rMulticast;

    uint64_t tBytes;
    uint64_t tPackets;
    uint64_t tErrs;
    uint64_t tDrop;
    uint64_t tFifo;
    uint64_t tColls;
    uint64_t tCarrier;
    uint64_t tCompressed;
};

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
