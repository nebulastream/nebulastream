/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>

namespace NES {
bool MetricUtils::validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t i) {
    if (i >= bufferSchema->getSize()) {
        return false;
    }

    auto hasName = Util::endsWith(bufferSchema->fields[i]->getName(), metricSchema->get(0)->getName());
    auto hasLastField = Util::endsWith(bufferSchema->fields[i + metricSchema->getSize() - 1]->getName(),
                                       metricSchema->get(metricSchema->getSize() - 1)->getName());

    if (!hasName || !hasLastField) {
        return false;
    }
    return true;
}

web::json::value MetricUtils::toJson(std::vector<MetricPtr> metrics) {
    web::json::value metricsJson{};
    for (const auto& metric : metrics) {
        auto jMetric = asJson(metric);
        metricsJson[toString(metric->getMetricType())] = jMetric;
    }
    return metricsJson;
}

web::json::value MetricUtils::toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics) {
    web::json::value metricsJson{};
    for (const auto& metric : metrics) {
        web::json::value jMetric = asJson(metric.second);
        metricsJson[toString(metric.second->getMetricType())] = jMetric;
    }
    return metricsJson;
}

}// namespace NES