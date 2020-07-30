#include <Monitoring/MetricCollector.hpp>
#include <Monitoring/Protocols/SamplingProtocol.hpp>
#include <Util/Logger.hpp>

namespace NES {

MetricCollector::MetricCollector(const MetricGroup& metricGroup, SamplingProtocolPtr samplingProtocol) : metricGroup(metricGroup), samplingProtocol(samplingProtocol) {
    NES_DEBUG("MetricCollector: Init()");
    NES_NOT_IMPLEMENTED();
}

}// namespace NES