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
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>

namespace NES::Monitoring {

DiskMetrics::DiskMetrics() : nodeId(0), fBsize(0), fFrsize(0), fBlocks(0), fBfree(0), fBavail(0), schema(getDefaultSchema("")) {}

DiskMetrics::DiskMetrics(SchemaPtr schema)
    : nodeId(0), fBsize(0), fFrsize(0), fBlocks(0), fBfree(0), fBavail(0), schema(schema) {}

SchemaPtr DiskMetrics::getDefaultSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

SchemaPtr DiskMetrics::createSchema(const std::string& prefix, const std::list<std::string>& configuredMetrics) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)->addField(prefix + "node_id", BasicType::UINT64);

    for (const auto& metric : configuredMetrics) {
        if (metric == "F_BSIZE") {
            schema->addField(prefix + "F_BSIZE", BasicType::UINT64);
        } else if (metric == "F_FRSIZE") {
            schema->addField(prefix + "F_FRSIZE", BasicType::UINT64);
        } else if (metric == "F_BLOCKS") {
            schema->addField(prefix + "F_BLOCKS", BasicType::UINT64);
        } else if (metric == "F_BFREE") {
            schema->addField(prefix + "F_BFREE", BasicType::UINT64);
        } else if (metric == "F_BAVAIL") {
            schema->addField(prefix + "F_BAVAIL", BasicType::UINT64);
        } else {
            NES_INFO("DiskMetrics: Metric unknown: " << metric);
        }
    }
    return schema;
}

void DiskMetrics::setSchema(SchemaPtr newSchema) { this->schema = std::move(newSchema); }

SchemaPtr DiskMetrics::getSchema() const { return this->schema; }

std::vector<std::string> DiskMetrics::getAttributesVector() {
    std::vector<std::string> attributesVector { "F_BAVAIL", "F_FRSIZE", "F_BSIZE", "F_BLOCKS", "F_BFREE" };

    return attributesVector;
}

uint64_t DiskMetrics::getValue(const std::string& metricName) const {
    if (metricName == "F_BSIZE") {
        return fBsize;
    } else if (metricName == "F_FRSIZE") {
        return fFrsize;
    } else if (metricName == "F_BLOCKS") {
        return fBlocks;
    } else if (metricName == "F_BFREE") {
        return fBfree;
    } else if (metricName == "F_BAVAIL") {
        return fBavail;
    } else if (metricName == "nodeId") {
        return nodeId;
    } else {
        NES_DEBUG("Unknown Metricname: " << metricName);
        //todo: find right exception
        throw std::exception();
    }
}

void DiskMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = this->schema->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "DiskMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    // TODO auffangen, wenn kein Schema da ist oder ein leeres
    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);//NodeId has always to be filled
    if (schema->contains("F_BSIZE")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(fBsize);
    }
    if (schema->contains("F_FRSIZE")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(fFrsize);
    }
    if (schema->contains("F_BLOCKS")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(fBlocks);
    }
    if (schema->contains("F_BFREE")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(fBfree);
    }
    if (schema->contains("F_BAVAIL")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(fBavail);
    }

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}
void DiskMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {//Nummer 5
    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    int cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    if (schema->contains("F_BSIZE")) {
        fBsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("F_FRSIZE")) {
        fFrsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("F_BLOCKS")) {
        fBlocks = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("F_BFREE")) {
        fBfree = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("F_BAVAIL")) {
        fBavail = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
}

//TODO: anpassen
web::json::value DiskMetrics::toJson() const {
    web::json::value metricsJson{};
    metricsJson["NODE_ID"] = web::json::value::number(nodeId);
    metricsJson["F_BSIZE"] = web::json::value::number(fBsize);
    metricsJson["F_FRSIZE"] = web::json::value::number(fFrsize);
    metricsJson["F_BLOCKS"] = web::json::value::number(fBlocks);
    metricsJson["F_BFREE"] = web::json::value::number(fBfree);
    metricsJson["F_BAVAIL"] = web::json::value::number(fBavail);
    return metricsJson;
}

bool DiskMetrics::operator==(const DiskMetrics& rhs) const {
    return nodeId == rhs.nodeId && fBavail == rhs.fBavail && fBfree == rhs.fBfree && fBlocks == rhs.fBlocks
        && fBsize == rhs.fBsize && fFrsize == rhs.fFrsize;
}

bool DiskMetrics::operator!=(const DiskMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const DiskMetrics& metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metric.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const DiskMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring