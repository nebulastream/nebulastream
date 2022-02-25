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
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void DiskMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    auto byteOffset = tupleIndex * sizeof(DiskMetrics);
    NES_ASSERT(byteOffset + sizeof(DiskMetrics) <= buf.getBufferSize(), "DiskMetrics: Content does not fit in TupleBuffer");
    memcpy(tbuffer + byteOffset, this, sizeof(DiskMetrics));
    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void DiskMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    //get index where the schema for CpuMetricsWrapper is starting
    auto schema = getSchema("");
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
    int cnt = 0;

    fBsize = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    fFrsize = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    fBlocks = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    fBfree = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    fBavail = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
}

web::json::value DiskMetrics::toJson() const {
    web::json::value metricsJson{};
    metricsJson["F_BSIZE"] = web::json::value::number(fBsize);
    metricsJson["F_FRSIZE"] = web::json::value::number(fFrsize);
    metricsJson["F_BLOCKS"] = web::json::value::number(fBlocks);
    metricsJson["F_BFREE"] = web::json::value::number(fBfree);
    metricsJson["F_BAVAIL"] = web::json::value::number(fBavail);
    return metricsJson;
}

bool DiskMetrics::operator==(const DiskMetrics& rhs) const {
    return fBavail == rhs.fBavail && fBfree == rhs.fBfree && fBlocks == rhs.fBlocks && fBsize == rhs.fBsize
        && fFrsize == rhs.fFrsize;
}

bool DiskMetrics::operator!=(const DiskMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const DiskMetrics& metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metric.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

web::json::value asJson(const DiskMetrics& metrics) {
    return metrics.toJson();
}

}// namespace NES