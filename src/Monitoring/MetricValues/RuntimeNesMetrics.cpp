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
#include <Common/DataTypes/FixedChar.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

RuntimeNesMetrics::RuntimeNesMetrics()
    : memoryUsageInBytes(0), cpuLoadInJiffies(0), blkioBytesRead(0), blkioBytesWritten(0), batteryStatusInPercent(0), latCoord(0),
      longCoord(0) {
    NES_DEBUG("RuntimeNesMetrics: Default ctor");
}

SchemaPtr RuntimeNesMetrics::getSchema(const std::string& prefix) {
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

RuntimeNesMetrics RuntimeNesMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    RuntimeNesMetrics output{};
    auto i = schema->getIndex(prefix + "wallTimeNs");

    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("RuntimeNesMetrics: Tuple size should be 1, but is larger "
                                + std::to_string(buf.getNumberOfTuples()));
    }

    if (!MetricUtils::validateFieldsInSchema(RuntimeNesMetrics::getSchema(""), schema, i)) {
        NES_THROW_RUNTIME_ERROR("RuntimeNesMetrics: Incomplete number of fields in schema.");
    }

    auto layout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.wallTimeNs = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.memoryUsageInBytes =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.cpuLoadInJiffies =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.blkioBytesRead = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.blkioBytesWritten =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.batteryStatusInPercent =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.latCoord = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.longCoord = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    return output;
}

web::json::value RuntimeNesMetrics::toJson() const {
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

bool RuntimeNesMetrics::operator==(const RuntimeNesMetrics& rhs) const {
    return wallTimeNs == rhs.wallTimeNs && memoryUsageInBytes == rhs.memoryUsageInBytes
        && cpuLoadInJiffies == rhs.cpuLoadInJiffies && blkioBytesRead == rhs.blkioBytesRead
        && blkioBytesWritten == rhs.blkioBytesWritten && batteryStatusInPercent == rhs.batteryStatusInPercent
        && latCoord == rhs.latCoord && longCoord == rhs.longCoord;
}

bool RuntimeNesMetrics::operator!=(const RuntimeNesMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const RuntimeNesMetrics& metric, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(RuntimeNesMetrics) < buf.getBufferSize(),
               "RuntimeNesMetrics: Content does not fit in TupleBuffer");
    NES_ASSERT(sizeof(RuntimeNesMetrics) == RuntimeNesMetrics::getSchema("")->getSchemaSizeInBytes(),
               sizeof(RuntimeNesMetrics) << "!=" << RuntimeNesMetrics::getSchema("")->getSchemaSizeInBytes());

    memcpy(tbuffer + byteOffset, &metric, sizeof(RuntimeNesMetrics));
    buf.setNumberOfTuples(1);
}

SchemaPtr getSchema(const RuntimeNesMetrics&, const std::string& prefix) { return RuntimeNesMetrics::getSchema(prefix); }

}// namespace NES
