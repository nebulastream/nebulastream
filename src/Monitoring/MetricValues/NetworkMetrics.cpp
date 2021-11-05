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
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cpprest/json.h>
#include <cstring>

namespace NES {

NetworkMetrics::NetworkMetrics() = default;

NetworkValues NetworkMetrics::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= getInterfaceNum()) {
        NES_THROW_RUNTIME_ERROR("CPU: ArrayType index out of bound " + std::to_string(interfaceNo)
                                + ">=" + std::to_string(getInterfaceNum()));
    }
    return networkValues.at(interfaceNo);
}

void NetworkMetrics::addNetworkValues(NetworkValues&& nwValue) {
    networkValues.emplace_back(nwValue);
    interfaceNum++;
}

uint64_t NetworkMetrics::getInterfaceNum() const {
    NES_ASSERT(interfaceNum == networkValues.size(), "NetworkMetrics: Interface numbers are not equal.");
    return interfaceNum;
}

std::vector<std::string> NetworkMetrics::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkValues.size());

    for (const auto& netVal : networkValues) {
        keys.push_back(std::to_string(netVal.interfaceName));
    }

    return keys;
}

NetworkMetrics NetworkMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    auto output = NetworkMetrics();
    auto i = schema->getIndex(prefix + "INTERFACE_NO");
    auto fieldName = schema->fields[i]->getName();
    bool hasField = Util::endsWith(fieldName, prefix + "INTERFACE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && hasField) {
        NES_DEBUG("NetworkMetrics: Prefix found in schema " + prefix + "INTERFACE_NO with index " + std::to_string(i));

        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, true);
        auto bindedRowLayout = layout->bind(buf);
        auto numInt = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i, bindedRowLayout)[0];

        for (auto n{0ul}; n < numInt; ++n) {
            NES_DEBUG("NetworkMetrics: Parsing buffer for interface " + prefix + "Intfs[" + std::to_string(n + 1) + "]_");
            output.addNetworkValues(NetworkValues::fromBuffer(schema, buf, prefix + "Intfs[" + std::to_string(n + 1) + "]_"));
        }
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetrics: Metrics could not be parsed from schema " + schema->toString());
    }

    return output;
}

web::json::value NetworkMetrics::toJson() {
    web::json::value metricsJson{};

    for (auto networkVal : networkValues) {
        metricsJson[networkVal.interfaceName] = networkVal.toJson();
    }

    return metricsJson;
}

void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    uint64_t intNum = metrics.getInterfaceNum();

    uint64_t totalSize = byteOffset + sizeof(uint64_t) + sizeof(NetworkValues) * intNum;
    NES_ASSERT(totalSize <= buf.getBufferSize(), "NetworkMetrics: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &intNum, sizeof(uint64_t));
    byteOffset += sizeof(uint64_t);

    for (uint64_t i = 0; i < intNum; i++) {
        writeToBuffer(metrics.getNetworkValue(i), buf, byteOffset);
        byteOffset += sizeof(NetworkValues);
    }

    buf.setNumberOfTuples(1);
}

SchemaPtr getSchema(const NetworkMetrics& metrics, const std::string& prefix) {
    auto schema = Schema::create();
    schema->addField(prefix + "INTERFACE_NO", BasicType::UINT64);

    for (uint64_t i = 0; i < metrics.getInterfaceNum(); i++) {
        auto interfacePrefix = prefix + "Intfs[" + std::to_string(i + 1) + "]_"
            + Util::trim(std::to_string(metrics.getNetworkValue(i).interfaceName)) + "_";

        schema->copyFields(NetworkValues::getSchema(interfacePrefix));
    }

    return schema;
}

bool NetworkMetrics::operator==(const NetworkMetrics& rhs) const {
    if (networkValues.size() != rhs.networkValues.size()) {
        return false;
    }

    for (auto i = static_cast<decltype(networkValues)::size_type>(0); i < networkValues.size(); ++i) {
        if (networkValues[i] != rhs.networkValues[i]) {
            return false;
        }
    }
    return true;
}

bool NetworkMetrics::operator!=(const NetworkMetrics& rhs) const { return !(rhs == *this); }

}// namespace NES