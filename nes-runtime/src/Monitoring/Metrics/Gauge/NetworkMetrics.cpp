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
#include <Common/DataTypes/FixedChar.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

NetworkMetrics::NetworkMetrics()
    : nodeId(0), timestamp(0), interfaceName(0), rBytes(0), rPackets(0), rErrs(0), rDrop(0), rFifo(0), rFrame(0), rCompressed(0),
      rMulticast(0), tBytes(0), tPackets(0), tErrs(0), tDrop(0), tFifo(0), tColls(0), tCarrier(0), tCompressed(0) {}

Configurations::SchemaTypePtr NetworkMetrics::getSchemaType(const std::string& prefix) {

    std::vector<Configurations::SchemaFieldDetail> schemaFiledDetails;
    const char* length = "0";
    const char* dataType = "UINT64";
    schemaFiledDetails.emplace_back(prefix + "node_id", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "timestamp", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "name", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rBytes", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rPackets", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rErrs", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rDrop", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rFifo", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rFrame", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rCompressed", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "rMulticast", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tBytes", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tPackets", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tErrs", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tDrop", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tFifo", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tColls", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tCarrier", dataType, length);
    schemaFiledDetails.emplace_back(prefix + "tCompressed", dataType, length);
    return Configurations::SchemaType::create(schemaFiledDetails);
}

SchemaPtr NetworkMetrics::getSchema(const std::string& prefix) { return Schema::createFromSchemaType(getSchemaType(prefix)); }

void NetworkMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto totalSize = NetworkMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "NetworkMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    auto layout = Runtime::MemoryLayouts::RowLayout::create(NetworkMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::TestTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestamp);

    buffer[tupleIndex][cnt++].write<uint64_t>(interfaceName);
    buffer[tupleIndex][cnt++].write<uint64_t>(rBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(rPackets);
    buffer[tupleIndex][cnt++].write<uint64_t>(rErrs);
    buffer[tupleIndex][cnt++].write<uint64_t>(rDrop);
    buffer[tupleIndex][cnt++].write<uint64_t>(rFifo);
    buffer[tupleIndex][cnt++].write<uint64_t>(rFrame);
    buffer[tupleIndex][cnt++].write<uint64_t>(rCompressed);
    buffer[tupleIndex][cnt++].write<uint64_t>(rMulticast);

    buffer[tupleIndex][cnt++].write<uint64_t>(tBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(tPackets);
    buffer[tupleIndex][cnt++].write<uint64_t>(tErrs);
    buffer[tupleIndex][cnt++].write<uint64_t>(tDrop);
    buffer[tupleIndex][cnt++].write<uint64_t>(tFifo);
    buffer[tupleIndex][cnt++].write<uint64_t>(tColls);
    buffer[tupleIndex][cnt++].write<uint64_t>(tCarrier);
    buffer[tupleIndex][cnt++].write<uint64_t>(tCompressed);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void NetworkMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(NetworkMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::TestTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestamp = buffer[tupleIndex][cnt++].read<uint64_t>();

    interfaceName = buffer[tupleIndex][cnt++].read<uint64_t>();
    rBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    rPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    rErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    rDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    rFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    rFrame = buffer[tupleIndex][cnt++].read<uint64_t>();
    rCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
    rMulticast = buffer[tupleIndex][cnt++].read<uint64_t>();

    tBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    tPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    tErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    tDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    tFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    tColls = buffer[tupleIndex][cnt++].read<uint64_t>();
    tCarrier = buffer[tupleIndex][cnt++].read<uint64_t>();
    tCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
}

nlohmann::json NetworkMetrics::toJson() const {
    nlohmann::json metricsJson{};

    metricsJson["NODE_ID"] = nodeId;
    metricsJson["TIMESTAMP"] = timestamp;

    metricsJson["R_BYTES"] = rBytes;
    metricsJson["R_PACKETS"] = rPackets;
    metricsJson["R_ERRS"] = rErrs;
    metricsJson["R_DROP"] = rDrop;
    metricsJson["R_FIFO"] = rFifo;
    metricsJson["R_FRAME"] = rFrame;
    metricsJson["R_COMPRESSED"] = rCompressed;
    metricsJson["R_MULTICAST"] = rMulticast;

    metricsJson["T_BYTES"] = tBytes;
    metricsJson["T_PACKETS"] = tPackets;
    metricsJson["T_ERRS"] = tErrs;
    metricsJson["T_DROP"] = tDrop;
    metricsJson["T_FIFO"] = tFifo;
    metricsJson["T_COLLS"] = tColls;
    metricsJson["T_CARRIER"] = tCarrier;
    metricsJson["T_COMPRESSED"] = tCompressed;

    return metricsJson;
}

bool NetworkMetrics::operator==(const NetworkMetrics& rhs) const {
    return nodeId == rhs.nodeId && timestamp == rhs.timestamp && interfaceName == rhs.interfaceName && rBytes == rhs.rBytes
        && rPackets == rhs.rPackets && rErrs == rhs.rErrs && rDrop == rhs.rDrop && rFifo == rhs.rFifo && rFrame == rhs.rFrame
        && rCompressed == rhs.rCompressed && rMulticast == rhs.rMulticast && tBytes == rhs.tBytes && tPackets == rhs.tPackets
        && tErrs == rhs.tErrs && tDrop == rhs.tDrop && tFifo == rhs.tFifo && tColls == rhs.tColls && tCarrier == rhs.tCarrier
        && tCompressed == rhs.tCompressed;
}
bool NetworkMetrics::operator!=(const NetworkMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const NetworkMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
