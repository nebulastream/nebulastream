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
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {
CpuMetricsWrapper::CpuMetricsWrapper() : schema(CpuMetrics::getDefaultSchema("")) {}

CpuMetricsWrapper::CpuMetricsWrapper(uint64_t nodeId) : nodeId(nodeId), schema(CpuMetrics::getDefaultSchema("")) {}

CpuMetricsWrapper::CpuMetricsWrapper(std::vector<CpuMetrics>&& arr) {
    if (!arr.empty()) {
        cpuMetrics = std::move(arr);
        schema = CpuMetrics::getDefaultSchema("");
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_TRACE("CpuMetricsWrapper: Allocating memory for " + std::to_string(arr.size()) + " metrics.");
}

CpuMetricsWrapper::CpuMetricsWrapper(uint64_t nodeId, SchemaPtr schema) : nodeId(nodeId), schema(std::move(schema)) {
    NES_DEBUG("CpuMetricsWrapper: Constructor: Allocating a CpuMetricsWrapper!");
    for (auto i = 0; i < (int) cpuMetrics.size(); i++) {
        cpuMetrics[i].setSchema(schema);
    }
}

CpuMetricsWrapper::CpuMetricsWrapper(std::vector<CpuMetrics>&& arr, SchemaPtr schemaNew) {
    if (!arr.empty()) {
        cpuMetrics = std::move(arr);
        for (auto i = 0; i < (int) cpuMetrics.size(); i++) {
            cpuMetrics[i].setSchema(schemaNew);
        }
        schema = std::move(schemaNew);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_TRACE("CpuMetricsWrapper: Allocating memory for " + std::to_string(arr.size()) + " metrics.");
}

CpuMetricsWrapper::CpuMetricsWrapper(SchemaPtr schema) : schema(std::move(schema)) {}

CpuMetrics CpuMetricsWrapper::getValue(const unsigned int cpuCore) const {
    CpuMetrics cpuMetric = cpuMetrics.at(cpuCore);
    cpuMetric.setSchema(this->schema);
    return cpuMetric;
}

void CpuMetricsWrapper::setSchema(SchemaPtr newSchema) {
    for (auto i = 0; i < (int) cpuMetrics.size(); i++) {
        cpuMetrics[i].setSchema(newSchema);
    }
    this->schema = std::move(newSchema);
}

SchemaPtr CpuMetricsWrapper::getSchema() const { return this->schema; }

void CpuMetricsWrapper::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto totalSize = schema->getSchemaSizeInBytes() * size();
    if (schema->getSchemaSizeInBytes() > buf.getBufferSize()) {
        NES_ERROR("CpuMetricsWrapper: At least one tuple of CpuMetrics has to fit into buffer");
    } else if (totalSize > buf.getBufferSize()) {
        NES_WARNING("CpuMetricsWrapper: Content does not fit in TupleBuffer totalSize:" << totalSize << " > "
                                                                                        << " getBufferSize:"
                                                                                        << buf.getBufferSize());
    }

    for (unsigned int i = 0; i < size(); i++) {
        CpuMetrics metrics = getValue(i);
        metrics.nodeId = nodeId;
        metrics.setSchema(this->schema);
        metrics.writeToBuffer(buf, tupleIndex + i);
    }
}

void CpuMetricsWrapper::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto cpuList = std::vector<CpuMetrics>();
    NES_TRACE("CpuMetricsWrapper: Parsing buffer with number of tuples " << buf.getNumberOfTuples());

    for (unsigned int n = 0; n < buf.getNumberOfTuples(); n++) {
        //for each core parse the according CpuMetrics
        CpuMetrics metrics{};
        metrics.setSchema(this->schema);
        NES::Monitoring::readFromBuffer(metrics, buf, tupleIndex + n);
        cpuList.emplace_back(metrics);
    }
    cpuMetrics = std::move(cpuList);
    nodeId = cpuMetrics[0].nodeId;
}

uint64_t CpuMetricsWrapper::size() const { return cpuMetrics.size(); }

CpuMetrics CpuMetricsWrapper::getTotal() const { return getValue(0); }

nlohmann::json CpuMetricsWrapper::toJson() const {
    nlohmann::json metricsJsonWrapper{};
    metricsJsonWrapper["NODE_ID"] = nodeId;
    SchemaPtr schemaTemp = this->schema;
    nlohmann::json metricsJson{};
    for (auto i = 0; i < (int) cpuMetrics.size(); i++) {
        auto coreNum = cpuMetrics[i].getValue("coreNum");
        NES_DEBUG("CpuMetricsWrapper: CORE_NUM is" + std::to_string(coreNum));
        if (coreNum == 0) {
            metricsJson["TOTAL"] = cpuMetrics[i].toJson();
        } else {
            metricsJson["CORE_" + std::to_string(coreNum)] = cpuMetrics[i].toJson();
        }
    }
    metricsJsonWrapper["values"] = metricsJson;
    return metricsJson;
}

void CpuMetricsWrapper::setCores(std::list<uint64_t> coresList) {
    std::vector<CpuMetrics> arrayNew;
    if (!(coresList.empty())){
        for (auto i : coresList) {
            if (i < cpuMetrics.size()) {
                arrayNew.push_back(cpuMetrics[i]);
            }
        }
        cpuMetrics = std::move(arrayNew);
    }
}

bool CpuMetricsWrapper::operator==(const CpuMetricsWrapper& rhs) const {
    if (cpuMetrics.size() != rhs.size()) {
        NES_ERROR("CpuMetricsWrapper: Sizes are not equal " << cpuMetrics.size() << "!=" << rhs.size());
        return false;
    }

    for (unsigned int i = 0; i < cpuMetrics.size(); i++) {
        if (cpuMetrics[i] != rhs.cpuMetrics[i]) {
            NES_ERROR("CpuMetricsWrapper: Cpu core " << i << " are not equal.");
            return false;
        }
    }

    if (nodeId != rhs.nodeId) {
        return false;
    }

    return true;
}

bool CpuMetricsWrapper::operator!=(const CpuMetricsWrapper& rhs) const { return !(rhs == *this); }

uint64_t CpuMetricsWrapper::getNodeId() const { return nodeId; }

void CpuMetricsWrapper::setNodeId(uint64_t nodeId) {
    this->nodeId = nodeId;
    if (!cpuMetrics.empty()) {
        for (auto& nMetric : cpuMetrics) {
            nMetric.nodeId = this->nodeId;
        }
    }
}

void writeToBuffer(const CpuMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(CpuMetricsWrapper& wrapper, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    wrapper.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const CpuMetricsWrapper& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring