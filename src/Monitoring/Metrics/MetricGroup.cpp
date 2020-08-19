#include <Monitoring/Metrics/MetricGroup.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

namespace NES {

MetricGroup::MetricGroup() {
    NES_INFO("MetricGroup: Ctor called");
}

std::shared_ptr<MetricGroup> MetricGroup::create() {
    return std::make_shared<MetricGroup>(MetricGroup());
}

bool MetricGroup::add(const std::string& desc, const Metric& metric) {
    return metricMap.insert(std::make_pair(desc, metric)).second;
}

bool MetricGroup::remove(const std::string& name) {
    return metricMap.erase(name);
}

void MetricGroup::getSample(std::shared_ptr<Schema> schema, TupleBuffer& buf) {
    NES_DEBUG("MetricGroup: Collecting sample via serialize(..)");
    for (auto const& x : metricMap) {
        serialize(x.second, schema, buf, x.first + "_");
    }
}

}// namespace NES