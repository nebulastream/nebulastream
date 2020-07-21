#include <Monitoring/Protocols/ReportingProtocol.hpp>

#include <Util/Logger.hpp>
namespace NES {

ReportingProtocol::ReportingProtocol(std::function<void(MetricGroup&)>&& reportingFunc) :
    reportingFunc(reportingFunc) {
    NES_DEBUG("ReportingProtocol: Init()");
}

bool ReportingProtocol::canReceive() const {
    return receiving;
}

void ReportingProtocol::receive(MetricGroup& metricGroup) {
    if (receiving) {
        NES_DEBUG("ReportingProtocol: Receiving metrics");
        reportingFunc(metricGroup);
    }
    else {
        NES_ERROR("ReportingProtocol: Metrics received, but not ready for receiving");
    }
}

}