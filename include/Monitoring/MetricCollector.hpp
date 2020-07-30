#ifndef NES_INCLUDE_MONITORING_METRICCOLLECTOR_HPP_
#define NES_INCLUDE_MONITORING_METRICCOLLECTOR_HPP_

#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Protocols/SamplingProtocol.hpp>

namespace NES {

/**
 * @brief WIP
 */
class MetricCollector {
  public:
    explicit MetricCollector(const MetricGroup& metricGroup, SamplingProtocolPtr samplingProtocol);

  private:
    const MetricGroup metricGroup;
    SamplingProtocolPtr samplingProtocol;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICCOLLECTOR_HPP_
