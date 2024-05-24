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
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {
NetworkMetricsWrapper::NetworkMetricsWrapper() : NetworkMetricsWrapper(INVALID_WORKER_NODE_ID) {}

NetworkMetricsWrapper::NetworkMetricsWrapper(WorkerId nodeId)
    : nodeId(nodeId),
      timestamp(duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count()) {}

NetworkMetricsWrapper::NetworkMetricsWrapper(std::vector<NetworkMetrics>&& arr) : NetworkMetricsWrapper() {
    if (!arr.empty()) {
        networkMetrics = std::move(arr);
        for (auto& metric : networkMetrics) {
            metric.timestamp = timestamp;
        }
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: Object cannot be allocated with less than 0 cores.");
    }
    NES_TRACE("NetworkMetricsWrapper: Allocating memory for {} metrics.", std::to_string(arr.size()));
}

void NetworkMetricsWrapper::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto schema = NetworkMetrics::getSchema("");
    auto totalSize = schema->getSchemaSizeInBytes() * size();
    if (schema->getSchemaSizeInBytes() > buf.getBufferSize()) {
        NES_ERROR("NetworkMetricsWrapper: At least one tuple of NetworkMetrics has to fit into buffer");
    } else if (totalSize > buf.getBufferSize()) {
        NES_WARNING("NetworkMetricsWrapper: Content does not fit in TupleBuffer totalSize: {} > getBufferSize: {}",
                    totalSize,
                    buf.getBufferSize());
    }

    for (unsigned int i = 0; i < size(); i++) {
        NetworkMetrics metrics = getNetworkValue(i);
        metrics.nodeId = nodeId;
        metrics.writeToBuffer(buf, tupleIndex + i);
    }
}

void NetworkMetricsWrapper::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto schema = NetworkMetrics::getSchema("");
    auto interfaceList = std::vector<NetworkMetrics>();
    NES_TRACE("NetworkMetricsWrapper: Parsing buffer with number of tuples {}", buf.getNumberOfTuples());

    for (unsigned int n = 0; n < buf.getNumberOfTuples(); n++) {
        //for each core parse the according CpuMetrics
        NetworkMetrics metrics{};
        NES::Monitoring::readFromBuffer(metrics, buf, tupleIndex + n);
        interfaceList.emplace_back(metrics);
    }
    networkMetrics = std::move(interfaceList);
    nodeId = networkMetrics[0].nodeId;
    timestamp = networkMetrics[0].timestamp;
}

NetworkMetrics NetworkMetricsWrapper::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= size()) {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: ArrayType index out of bound " + std::to_string(interfaceNo)
                                + ">=" + std::to_string(size()));
    }
    return networkMetrics.at(interfaceNo);
}

void NetworkMetricsWrapper::addNetworkMetrics(NetworkMetrics&& nwValue) {
    nwValue.nodeId = this->nodeId;
    nwValue.timestamp = this->timestamp;
    networkMetrics.emplace_back(nwValue);
}

uint64_t NetworkMetricsWrapper::size() const { return networkMetrics.size(); }

std::vector<std::string> NetworkMetricsWrapper::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkMetrics.size());

    for (const auto& netVal : networkMetrics) {
        keys.push_back(std::to_string(netVal.interfaceName));
    }

    return keys;
}

nlohmann::json NetworkMetricsWrapper::toJson() const {
    nlohmann::json metricsJsonWrapper{};
    metricsJsonWrapper["NODE_ID"] = nodeId;
    metricsJsonWrapper["TIMESTAMP"] = timestamp;

    nlohmann::json metricsJson{};
    for (auto networkVal : networkMetrics) {
        metricsJson[networkVal.interfaceName] = networkVal.toJson();
    }
    metricsJsonWrapper["values"] = metricsJson;
    return metricsJsonWrapper;
}

bool NetworkMetricsWrapper::operator==(const NetworkMetricsWrapper& rhs) const {
    if (networkMetrics.size() != rhs.networkMetrics.size()) {
        return false;
    }

    for (auto i = static_cast<decltype(networkMetrics)::size_type>(0); i < networkMetrics.size(); ++i) {
        if (networkMetrics[i] != rhs.networkMetrics[i]) {
            return false;
        }
    }

    if (nodeId != rhs.nodeId) {
        return false;
    }

    return true;
}

bool NetworkMetricsWrapper::operator!=(const NetworkMetricsWrapper& rhs) const { return !(rhs == *this); }

WorkerId NetworkMetricsWrapper::getNodeId() const { return nodeId; }

void NetworkMetricsWrapper::setNodeId(WorkerId nodeId) {
    this->nodeId = nodeId;
    if (!networkMetrics.empty()) {
        for (auto& nMetric : networkMetrics) {
            nMetric.nodeId = this->nodeId;
        }
    }
}

void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const NetworkMetricsWrapper& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
