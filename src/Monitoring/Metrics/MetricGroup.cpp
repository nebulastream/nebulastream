#include <Monitoring/Metrics/MetricGroup.hpp>

#include <Util/Logger.hpp>
#include <API/Schema.hpp>

namespace NES {

MetricGroup::MetricGroup(): schema(Schema::create()) {
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

std::shared_ptr<Schema> MetricGroup::getSchema() {
    return schema;
}

}// namespace NES