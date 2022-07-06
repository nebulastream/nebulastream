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

namespace NES {

DiskMetrics::DiskMetrics() : nodeId(0), fBsize(0), fFrsize(0), fBlocks(0), fBfree(0), fBavail(0) {}

DiskMetrics::DiskMetrics(int test) : nodeId(0), fBsize(0), fFrsize(0), fBlocks(0), fBfree(0) {
    std::cout << test;
}

//Konstruktor der nen Schema hat mit den gewünschten Metriken

SchemaPtr DiskMetrics::getSchemaBA01(const std::string& prefix) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64);
    return schema;
}

SchemaPtr DiskMetrics::getSchemaBA02(const std::string& prefix, std::list<std::string> configuredMetrics) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64);
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


SchemaPtr DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void DiskMetrics::writeToBufferBA02(Runtime::TupleBuffer& buf, uint64_t tupleIndex, SchemaPtr schema) const {                 //auch dynamisch machen
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = schema->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "DiskMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);          //NodeId has always to be filled
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

void DiskMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {                 //auch dynamisch machen
    auto layout = Runtime::MemoryLayouts::RowLayout::create(DiskMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = DiskMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "DiskMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBsize);
    buffer[tupleIndex][cnt++].write<uint64_t>(fFrsize);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBlocks);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBfree);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBavail);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void DiskMetrics::readFromBufferBA02(Runtime::TupleBuffer& buf, uint64_t tupleIndex, SchemaPtr schema) {                  //auch dynamisch
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
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


void DiskMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {                  //auch dynamisch
    auto layout = Runtime::MemoryLayouts::RowLayout::create(DiskMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    int cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    fFrsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBlocks = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBfree = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBavail = buffer[tupleIndex][cnt++].read<uint64_t>();
}

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

void writeToBufferBA02(const DiskMetrics& metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex, SchemaPtr schema) {
    metric.writeToBufferBA02(buf, tupleIndex, schema);
}

void readFromBufferBA02(DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex, SchemaPtr schema) {
    metrics.readFromBufferBA02(buf, tupleIndex, schema);
}

web::json::value asJson(const DiskMetrics& metrics) { return metrics.toJson(); }

}// namespace NES