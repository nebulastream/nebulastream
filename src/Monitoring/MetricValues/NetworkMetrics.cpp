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

#include <API/Schema.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cstring>

namespace NES {

NetworkValues NetworkMetrics::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= getInterfaceNum()) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(interfaceNo)
                                + ">=" + std::to_string(getInterfaceNum()));
    }
    return networkValues.at(interfaceNo);
}

void NetworkMetrics::addNetworkValues(NetworkValues&& nwValue) { networkValues.emplace_back(nwValue); }

uint64_t NetworkMetrics::getInterfaceNum() const { return networkValues.size(); }

std::vector<std::string> NetworkMetrics::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkValues.size());

    for (const auto& netVal : networkValues) {
        keys.push_back(netVal.interfaceName);
    }

    return keys;
}

NetworkMetrics NetworkMetrics::fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    auto output = NetworkMetrics();
    auto i = schema->getIndex(prefix + "INTERFACE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1
        && UtilityFunctions::endsWith(schema->fields[i]->getName(), prefix + "INTERFACE_NO")) {
        NES_DEBUG("NetworkMetrics: Prefix found in schema " + prefix + "INTERFACE_NO with index " + std::to_string(i));
        auto layout = NodeEngine::createRowLayout(schema);
        auto numInt = layout->getValueField<uint16_t>(0, i)->read(buf);

        for (int n = 0; n < numInt; n++) {
            NES_DEBUG("NetworkMetrics: Parsing buffer for interface " + prefix + "Intfs[" + std::to_string(n + 1) + "]_");

            auto networkValue = NetworkValues::fromBuffer(schema, buf, prefix + "Intfs[" + std::to_string(n + 1) + "]_");
            output.addNetworkValues(std::move(networkValue));
        }
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetrics: Metrics could not be parsed from schema " + schema->toString());
    }

    return output;
}

void writeToBuffer(const NetworkMetrics& metrics, NodeEngine::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBufferAs<uint8_t>();
    auto intNum = metrics.getInterfaceNum();

    uint64_t totalSize = byteOffset + sizeof(uint64_t) + sizeof(NetworkValues) * intNum;
    NES_ASSERT(totalSize < buf.getBufferSize(), "NetworkMetrics: Content does not fit in TupleBuffer");

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
    schema->addField(prefix + "INTERFACE_NO", BasicType::UINT16);

    for (uint64_t i = 0; i < metrics.getInterfaceNum(); i++) {
        auto interfacePrefix = prefix + "Intfs[" + std::to_string(i + 1) + "]_"
            + UtilityFunctions::trim(metrics.getNetworkValue(i).interfaceName) + "_";

        schema->copyFields(NetworkValues::getSchema(interfacePrefix));
    }

    return schema;
}

}// namespace NES