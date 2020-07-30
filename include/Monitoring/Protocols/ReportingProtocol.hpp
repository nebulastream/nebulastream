#ifndef NES_INCLUDE_MONITORING_PROTOCOLS_REPORTINGPROTOCOL_HPP_
#define NES_INCLUDE_MONITORING_PROTOCOLS_REPORTINGPROTOCOL_HPP_

#include <Monitoring/Metrics/MetricGroup.hpp>
#include <functional>

namespace NES {

class ReportingProtocol {
  public:
    ReportingProtocol(std::function<void(MetricGroup&)>&& reportingFunc);

    bool canReceive() const;

    void receive(MetricGroup& metricGroup);

  private:
    std::function<void(MetricGroup&)> reportingFunc;
    bool receiving = true;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_PROTOCOLS_REPORTINGPROTOCOL_HPP_
