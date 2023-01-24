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
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>

namespace NES::Monitoring {

NetworkMetrics::NetworkMetrics()
    : nodeId(0), interfaceName(0), rBytes(0), rPackets(0), rErrs(0), rDrop(0), rFifo(0), rFrame(0), rCompressed(0), rMulticast(0),
      tBytes(0), tPackets(0), tErrs(0), tDrop(0), tFifo(0), tColls(0), tCarrier(0), tCompressed(0), schema(getDefaultSchema("")) {}

NetworkMetrics::NetworkMetrics(SchemaPtr schema)
    : nodeId(0), interfaceName(0), rBytes(0), rPackets(0), rErrs(0), rDrop(0), rFifo(0), rFrame(0), rCompressed(0), rMulticast(0),
      tBytes(0), tPackets(0), tErrs(0), tDrop(0), tFifo(0), tColls(0), tCarrier(0), tCompressed(0), schema(std::move(schema)) {}

NES::SchemaPtr NetworkMetrics::getDefaultSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    NES::SchemaPtr schema = NES::Schema::create(NES::Schema::ROW_LAYOUT)
                                ->addField(prefix + "node_id", BasicType::UINT64)

                                ->addField(prefix + "name", BasicType::UINT64)
                                ->addField(prefix + "rBytes", BasicType::UINT64)
                                ->addField(prefix + "rPackets", BasicType::UINT64)
                                ->addField(prefix + "rErrs", BasicType::UINT64)
                                ->addField(prefix + "rDrop", BasicType::UINT64)
                                ->addField(prefix + "rFifo", BasicType::UINT64)
                                ->addField(prefix + "rFrame", BasicType::UINT64)
                                ->addField(prefix + "rCompressed", BasicType::UINT64)
                                ->addField(prefix + "rMulticast", BasicType::UINT64)

                                ->addField(prefix + "tBytes", BasicType::UINT64)
                                ->addField(prefix + "tPackets", BasicType::UINT64)
                                ->addField(prefix + "tErrs", BasicType::UINT64)
                                ->addField(prefix + "tDrop", BasicType::UINT64)
                                ->addField(prefix + "tFifo", BasicType::UINT64)
                                ->addField(prefix + "tColls", BasicType::UINT64)
                                ->addField(prefix + "tCarrier", BasicType::UINT64)
                                ->addField(prefix + "tCompressed", BasicType::UINT64);

    return schema;
}

SchemaPtr NetworkMetrics::createSchema(const std::string& prefix, const std::list<std::string>& configuredMetrics) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "interfaceName", BasicType::UINT64);

    for (const auto& metric : configuredMetrics) {
        if (metric == "rBytes") {
            schema->addField(prefix + "rBytes", BasicType::UINT64);
        } else if (metric == "rPackets") {
            schema->addField(prefix + "rPackets", BasicType::UINT64);
        } else if (metric == "rErrs") {
            schema->addField(prefix + "rErrs", BasicType::UINT64);
        } else if (metric == "rDrop") {
            schema->addField(prefix + "rDrop", BasicType::UINT64);
        } else if (metric == "rFifo") {
            schema->addField(prefix + "rFifo", BasicType::UINT64);
        } else if (metric == "rFrame") {
            schema->addField(prefix + "rFrame", BasicType::UINT64);
        } else if (metric == "rCompressed") {
            schema->addField(prefix + "rCompressed", BasicType::UINT64);
        } else if (metric == "rMulticast") {
            schema->addField(prefix + "rMulticast", BasicType::UINT64);
        } else if (metric == "tBytes") {
            schema->addField(prefix + "tBytes", BasicType::UINT64);
        } else if (metric == "tPackets") {
            schema->addField(prefix + "tPackets", BasicType::UINT64);
        } else if (metric == "tErrs") {
            schema->addField(prefix + "tErrs", BasicType::UINT64);
        } else if (metric == "tDrop") {
            schema->addField(prefix + "tDrop", BasicType::UINT64);
        } else if (metric == "tFifo") {
            schema->addField(prefix + "tFifo", BasicType::UINT64);
        } else if (metric == "tColls") {
            schema->addField(prefix + "tColls", BasicType::UINT64);
        } else if (metric == "tCarrier") {
            schema->addField(prefix + "tCarrier", BasicType::UINT64);
        } else if (metric == "tCompressed") {
            schema->addField(prefix + "tCompressed", BasicType::UINT64);
        } else {
            NES_INFO("DiskMetrics: Metric unknown: " << metric);
        }
    }
    return schema;
}

void NetworkMetrics::setSchema(SchemaPtr newSchema) { this->schema = std::move(newSchema); }

SchemaPtr NetworkMetrics::getSchema() const { return this->schema; }

std::vector<std::string> NetworkMetrics::getAttributesVector() {
    std::vector<std::string> attributesVector { "rBytes", "rPackets", "rErrs", "rDrop", "rFifo", "rFrame", "rCompressed",
                                              "rMulticast", "tBytes", "tPackets", "tErrs", "tDrop", "tFifo", "tColls", "tCarrier",
                                              "tCompressed"};
    return attributesVector;
}

uint64_t NetworkMetrics::getValue(const std::string& metricName) const {
    if (metricName == "interfaceName") {
        return interfaceName;
    } else if (metricName == "rBytes") {
        return rBytes;
    } else if (metricName == "rPackets") {
        return rPackets;
    } else if (metricName == "rErrs") {
        return rErrs;
    } else if (metricName == "rDrop") {
        return rDrop;
    } else if (metricName == "rFifo") {
        return rFifo;
    } else if (metricName == "rFrame") {
        return rFrame;
    } else if (metricName == "rCompressed") {
        return rCompressed;
    } else if (metricName == "rMulticast") {
        return rMulticast;
    } else if (metricName == "tBytes") {
        return tBytes;
    } else if (metricName == "tPackets") {
        return tPackets;
    } else if (metricName == "tErrs") {
        return tErrs;
    } else if (metricName == "tDrop") {
        return tDrop;
    } else if (metricName == "tFifo") {
        return tFifo;
    } else if (metricName == "tColls") {
        return tColls;
    } else if (metricName == "tCarrier") {
        return tCarrier;
    } else if (metricName == "tCompressed") {
        return tCompressed;
    } else if (metricName == "nodeId") {
        return nodeId;
    }else {
        NES_DEBUG("Unknown Metricname: " << metricName);
        throw std::exception();
    }
}

void NetworkMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto totalSize = this->schema->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "NetworkMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(interfaceName);
    if (schema->contains("rBytes")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rBytes);
    }
    if (schema->contains("rPackets")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rPackets);
    }
    if (schema->contains("rErrs")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rErrs);
    }
    if (schema->contains("rDrop")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rDrop);
    }
    if (schema->contains("rFifo")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rFifo);
    }
    if (schema->contains("rFrame")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rFrame);
    }
    if (schema->contains("rCompressed")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rCompressed);
    }
    if (schema->contains("rMulticast")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(rMulticast);
    }
    if (schema->contains("tBytes")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tBytes);
    }
    if (schema->contains("tPackets")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tPackets);
    }
    if (schema->contains("tErrs")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tErrs);
    }
    if (schema->contains("tDrop")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tDrop);
    }
    if (schema->contains("tFifo")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tFifo);
    }
    if (schema->contains("tColls")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tColls);
    }
    if (schema->contains("tCarrier")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tCarrier);
    }
    if (schema->contains("tCompressed")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(tCompressed);
    }

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void NetworkMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    interfaceName = buffer[tupleIndex][cnt++].read<uint64_t>();

    if (schema->contains("rBytes")) {
        rBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rPackets")) {
        rPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rErrs")) {
        rErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rDrop")) {
        rDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rFifo")) {
        rFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rFrame")) {
        rFrame = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rCompressed")) {
        rCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("rMulticast")) {
        rMulticast = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tBytes")) {
        tBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tPackets")) {
        tPackets = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tErrs")) {
        tErrs = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tDrop")) {
        tDrop = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tFifo")) {
        tFifo = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tColls")) {
        tColls = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tCarrier")) {
        tCarrier = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("tCompressed")) {
        tCompressed = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
}

web::json::value NetworkMetrics::toJson() const {
    web::json::value metricsJson{};

    metricsJson["NODE_ID"] = web::json::value::number(nodeId);
    metricsJson["INTERFACENAME"] = web::json::value::number(interfaceName);

    if (schema->contains("rBytes")) {
        metricsJson["R_BYTES"] = web::json::value::number(rBytes);
    }
    if (schema->contains("rPackets")) {
        metricsJson["R_PACKETS"] = web::json::value::number(rPackets);
    }
    if (schema->contains("rErrs")) {
        metricsJson["R_ERRS"] = web::json::value::number(rErrs);
    }
    if (schema->contains("rDrop")) {
        metricsJson["R_DROP"] = web::json::value::number(rDrop);
    }
    if (schema->contains("rFifo")) {
        metricsJson["R_FIFO"] = web::json::value::number(rFifo);
    }
    if (schema->contains("rFrame")) {
        metricsJson["R_FRAME"] = web::json::value::number(rFrame);
    }
    if (schema->contains("rCompressed")) {
        metricsJson["R_COMPRESSED"] = web::json::value::number(rCompressed);
    }
    if (schema->contains("rMulticast")) {
        metricsJson["R_MULTICAST"] = web::json::value::number(rMulticast);
    }
    if (schema->contains("tBytes")) {
        metricsJson["T_BYTES"] = web::json::value::number(tBytes);
    }
    if (schema->contains("tPackets")) {
        metricsJson["T_PACKETS"] = web::json::value::number(tPackets);
    }
    if (schema->contains("tErrs")) {
        metricsJson["T_ERRS"] = web::json::value::number(tErrs);
    }
    if (schema->contains("tDrop")) {
        metricsJson["T_DROP"] = web::json::value::number(tDrop);
    }
    if (schema->contains("tFifo")) {
        metricsJson["T_FIFO"] = web::json::value::number(tFifo);
    }
    if (schema->contains("tColls")) {
        metricsJson["T_COLLS"] = web::json::value::number(tColls);
    }
    if (schema->contains("tCarrier")) {
        metricsJson["T_CARRIER"] = web::json::value::number(tCarrier);
    }
    if (schema->contains("tCompressed")) {
        metricsJson["T_COMPRESSED"] = web::json::value::number(tCompressed);
    }

    return metricsJson;
}

bool NetworkMetrics::operator==(const NetworkMetrics& rhs) const {
    return nodeId == rhs.nodeId && interfaceName == rhs.interfaceName && rBytes == rhs.rBytes && rPackets == rhs.rPackets
        && rErrs == rhs.rErrs && rDrop == rhs.rDrop && rFifo == rhs.rFifo && rFrame == rhs.rFrame
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

web::json::value asJson(const NetworkMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
