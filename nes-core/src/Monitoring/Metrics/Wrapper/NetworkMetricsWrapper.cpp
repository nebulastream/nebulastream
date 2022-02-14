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

NetworkMetricsWrapper
NetworkMetricsWrapper::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& schemaPrefix) {
    auto output = NetworkMetricsWrapper();
    auto networkMetricsSchema = NetworkMetrics::getSchema(schemaPrefix);
    auto firstFieldName = networkMetricsSchema->get(0)->getName();

    auto i = schema->getIndex(schemaPrefix + firstFieldName);
    auto fieldName = schema->fields[i]->getName();
    bool hasField = Util::endsWith(fieldName, schemaPrefix + "INTERFACE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && hasField) {
        NES_DEBUG("NetworkMetricsWrapper: Prefix found in schema " + schemaPrefix + "INTERFACE_NO with index "
                  + std::to_string(i));

        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
        auto numInt = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i, layout, buf)[0];

        for (auto n{0UL}; n < numInt; ++n) {
            NES_DEBUG("NetworkMetricsWrapper: Parsing buffer for interface " + schemaPrefix + "Intfs[" + std::to_string(n + 1)
                      + "]_");
            output.addNetworkMetrics(
                NetworkMetrics::fromBuffer(schema, buf, schemaPrefix + "Intfs[" + std::to_string(n + 1) + "]_"));
        }
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetricsWrapper: Metrics could not be parsed from schema " + schema->toString());
    }

    return output;
}

web::json::value NetworkMetricsWrapper::toJson() {
    web::json::value metricsJson{};

    for (auto networkVal : networkMetrics) {
        metricsJson[networkVal.interfaceName] = networkVal.toJson();
    }

    return metricsJson;
}

void writeToBuffer(const NetworkMetricsWrapper& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    uint64_t intNum = metrics.size();

    uint64_t totalSize = byteOffset + sizeof(uint64_t) + sizeof(NetworkMetrics) * intNum;
    NES_ASSERT(totalSize <= buf.getBufferSize(), "NetworkMetricsWrapper: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &intNum, sizeof(uint64_t));
    byteOffset += sizeof(uint64_t);

    for (uint64_t i = 0; i < intNum; i++) {
        writeToBuffer(metrics.getNetworkValue(i), buf, byteOffset);
        byteOffset += sizeof(NetworkMetrics);
    }

    buf.setNumberOfTuples(1);
}

SchemaPtr getSchema(const NetworkMetricsWrapper&, const std::string& prefix) { return NetworkMetrics::getSchema(prefix); }

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

}// namespace NES