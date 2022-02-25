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
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cpprest/json.h>
#include <cstring>

namespace NES {

void NetworkMetricsWrapper::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto schema = NetworkMetrics::getSchema("");
    auto totalSize = schema->getSchemaSizeInBytes() * size();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "CpuMetricsWrapper: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    for (unsigned int i = 0; i < size(); i++) {
        getNetworkValue(i).writeToBuffer(buf, tupleIndex + i);
    }
}

void NetworkMetricsWrapper::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto schema = NetworkMetrics::getSchema("");
    auto interfaceList = std::vector<NetworkMetrics>();
    NES_DEBUG("CpuMetricsWrapper: Parsing buffer with number of tuples " << buf.getNumberOfTuples());

    for (unsigned int n = 0; n < buf.getNumberOfTuples(); n++) {
        //for each core parse the according CpuMetrics
        NetworkMetrics metrics{};
        NES::readFromBuffer(metrics, buf, tupleIndex + n);
        interfaceList.emplace_back(metrics);
    }
    networkMetrics = std::move(interfaceList);
}

NetworkMetrics NetworkMetricsWrapper::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= size()) {
        NES_THROW_RUNTIME_ERROR("CPU: ArrayType index out of bound " + std::to_string(interfaceNo)
                                + ">=" + std::to_string(size()));
    }
    return networkMetrics.at(interfaceNo);
}

void NetworkMetricsWrapper::addNetworkMetrics(NetworkMetrics&& nwValue) { networkMetrics.emplace_back(nwValue); }

uint64_t NetworkMetricsWrapper::size() const { return networkMetrics.size(); }

std::vector<std::string> NetworkMetricsWrapper::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkMetrics.size());

    for (const auto& netVal : networkMetrics) {
        keys.push_back(std::to_string(netVal.interfaceName));
    }

    return keys;
}

web::json::value NetworkMetricsWrapper::toJson() const {
    web::json::value metricsJson{};

    for (auto networkVal : networkMetrics) {
        metricsJson[networkVal.interfaceName] = networkVal.toJson();
    }

    return metricsJson;
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
    return true;
}

bool NetworkMetricsWrapper::operator!=(const NetworkMetricsWrapper& rhs) const { return !(rhs == *this); }

void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const NetworkMetricsWrapper& metrics) { return metrics.toJson(); }

}// namespace NES