#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
#include <cstdint>
#include <memory>

namespace NES {
class Schema;
class TupleBuffer;

class NetworkValues {
  public:
    NetworkValues() = default;

    static std::shared_ptr<Schema> getSchema(const std::string& prefix);

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

void serialize(NetworkValues, std::shared_ptr<Schema>, TupleBuffer&, const std::string& prefix="");

}

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKVALUES_HPP_
