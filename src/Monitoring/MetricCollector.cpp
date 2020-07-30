#include <Monitoring/MetricCollector.hpp>
#include <Util/Logger.hpp>
#include <Monitoring/Protocols/SamplingProtocol.hpp>

namespace NES {

MetricCollector::MetricCollector(const MetricGroup& metricGroup, SamplingProtocolPtr samplingProtocol):
metricGroup(metricGroup), samplingProtocol(samplingProtocol){
    NES_DEBUG("MetricCollector: Init()");
    NES_NOT_IMPLEMENTED();
}

}