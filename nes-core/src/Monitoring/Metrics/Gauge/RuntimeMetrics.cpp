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
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

RuntimeMetrics::RuntimeMetrics()
    : wallTimeNs(0), memoryUsageInBytes(0), cpuLoadInJiffies(0), blkioBytesRead(0), blkioBytesWritten(0),
      batteryStatusInPercent(0), latCoord(0), longCoord(0) {
    NES_DEBUG("RuntimeMetrics: Default ctor");
}

SchemaPtr RuntimeMetrics::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "wallTimeNs", BasicType::UINT64)
                           ->addField(prefix + "memoryUsageInBytes", BasicType::UINT64)
                           ->addField(prefix + "cpuLoadInJiffies", BasicType::UINT64)
                           ->addField(prefix + "blkioBytesRead", BasicType::UINT64)
                           ->addField(prefix + "blkioBytesWritten", BasicType::UINT64)
                           ->addField(prefix + "batteryStatusInPercent", BasicType::UINT64)
                           ->addField(prefix + "latCoord", BasicType::UINT64)
                           ->addField(prefix + "longCoord", BasicType::UINT64);

    return schema;
}

void RuntimeMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    auto byteOffset = tupleIndex * sizeof(RuntimeMetrics);
    NES_ASSERT(byteOffset + sizeof(RuntimeMetrics) <= buf.getBufferSize(), "RuntimeMetrics: Content does not fit in TupleBuffer");
    NES_ASSERT(sizeof(RuntimeMetrics) == RuntimeMetrics::getSchema("")->getSchemaSizeInBytes(),
               sizeof(RuntimeMetrics) << "!=" << RuntimeMetrics::getSchema("")->getSchemaSizeInBytes());

    memcpy(tbuffer + byteOffset, this, sizeof(RuntimeMetrics));
    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void RuntimeMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    //get index where the schema is starting
    auto schema = getSchema("");
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());

    int cnt = 0;

    wallTimeNs = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    memoryUsageInBytes = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    cpuLoadInJiffies = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    blkioBytesRead = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    blkioBytesWritten = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    batteryStatusInPercent = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    latCoord = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    longCoord = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
}

web::json::value RuntimeMetrics::toJson() const {
    web::json::value metricsJson{};

    metricsJson["WallClockNs"] = web::json::value::number(wallTimeNs);
    metricsJson["MemoryUsageInBytes"] = web::json::value::number(memoryUsageInBytes);
    metricsJson["CpuLoadInJiffies"] = web::json::value::number(cpuLoadInJiffies);
    metricsJson["BlkioBytesRead"] = web::json::value::number(blkioBytesRead);
    metricsJson["BlkioBytesWritten"] = web::json::value::number(blkioBytesWritten);
    metricsJson["BatteryStatus"] = web::json::value::number(batteryStatusInPercent);
    metricsJson["LatCoord"] = web::json::value::number(latCoord);
    metricsJson["LongCoord"] = web::json::value::number(longCoord);

    return metricsJson;
}

bool RuntimeMetrics::operator==(const RuntimeMetrics& rhs) const {
    return wallTimeNs == rhs.wallTimeNs && memoryUsageInBytes == rhs.memoryUsageInBytes
        && cpuLoadInJiffies == rhs.cpuLoadInJiffies && blkioBytesRead == rhs.blkioBytesRead
        && blkioBytesWritten == rhs.blkioBytesWritten && batteryStatusInPercent == rhs.batteryStatusInPercent
        && latCoord == rhs.latCoord && longCoord == rhs.longCoord;
}

bool RuntimeMetrics::operator!=(const RuntimeMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

}// namespace NES
