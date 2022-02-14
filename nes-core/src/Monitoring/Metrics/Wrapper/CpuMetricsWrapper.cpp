/*
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
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cpprest/json.h>

namespace NES {
CpuMetricsWrapper::CpuMetricsWrapper(std::vector<CpuMetrics>&& arr) {
    if (!arr.empty()) {
        cpuMetrics = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_DEBUG("CpuMetricsWrapper: Allocating memory for " + std::to_string(arr.size()) + " metrics.");
}

CpuMetrics CpuMetricsWrapper::getValue(const unsigned int cpuCore) const { return cpuMetrics.at(cpuCore); }

CpuMetricsWrapper
CpuMetricsWrapper::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& schemaPrefix) {
    //get index where the schema for CpuMetricsWrapper is starting
    std::vector<CpuMetrics> output;
    auto cpuMetricsSchema = CpuMetrics::getSchema(schemaPrefix);
    auto firstFieldName = cpuMetricsSchema->get(0)->getName();
    auto idx = schema->getIndex(schemaPrefix + firstFieldName);

    if (idx < schema->getSize() && buf.getNumberOfTuples() == 1
        && Util::endsWith(schema->fields[idx]->getName(), firstFieldName)) {
        //if schema contains cpuMetrics parse the wrapper object
        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
        auto numCores = Runtime::MemoryLayouts::RowLayoutField<uint16_t, true>::create(idx, layout, buf)[0];

        auto cpu = std::vector<CpuMetrics>(numCores);

        for (int n = 0; n < numCores; n++) {
            //for each core parse the according CpuMetrics
            output.emplace_back(CpuMetrics::fromBuffer(schema, buf, schemaPrefix + "CPU[" + std::to_string(n + 1) + "]_"));
        }
        return output;
    }
    NES_THROW_RUNTIME_ERROR("CpuMetricsWrapper: Prefix " + schemaPrefix + " could not be parsed from schema "
                            + schema->toString());
}

web::json::value CpuMetricsWrapper::toJson() {
    web::json::value metricsJson{};

    for (auto i = 0; i < (int) cpuMetrics.size(); i++) {
        if (i == 0) {
            metricsJson["TOTAL"] = cpuMetrics[i].toJson();
        } else {
            metricsJson["CORE_" + std::to_string(i)] = cpuMetrics[i].toJson();
        }
    }
    return metricsJson;
}

void writeToBuffer(const CpuMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto totalSize = sizeof(CpuMetrics) * metrics.size();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "CpuMetricsWrapper: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    for (unsigned int i = 0; i < metrics.size(); i++) {
        writeToBuffer(metrics.getValue(i), buf, byteOffset);
        byteOffset += sizeof(CpuMetrics);
    }

    buf.setNumberOfTuples(metrics.size());
}

SchemaPtr getSchema(const CpuMetricsWrapper&, const std::string& prefix) { return CpuMetrics::getSchema(prefix); }

bool CpuMetricsWrapper::operator==(const CpuMetricsWrapper& rhs) const {
    if (cpuMetrics.size() != rhs.size()) {
        return false;
    }

    for (auto i = static_cast<decltype(cpuMetrics)::size_type>(0); i < cpuMetrics.size(); ++i) {
        if (cpuMetrics[i] != rhs.cpuMetrics[i]) {
            return false;
        }
    }
    return true;
}

bool CpuMetricsWrapper::operator!=(const CpuMetricsWrapper& rhs) const { return !(rhs == *this); }

uint64_t CpuMetricsWrapper::size() const { return cpuMetrics.size(); }

CpuMetrics CpuMetricsWrapper::getTotal() const { return getValue(0); }

}// namespace NES