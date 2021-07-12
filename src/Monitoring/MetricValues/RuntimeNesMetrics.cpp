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

#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>

#include <API/Schema.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr RuntimeNesMetrics::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    SchemaPtr schema = Schema::create()
        ->addField(prefix + "memoryUsageInBytes", BasicType::UINT64)
        ->addField(prefix + "cpuLoadInJiffies", BasicType::UINT64)
        ->addField(prefix + "blkioBytesRead", BasicType::UINT64)
        ->addField(prefix + "blkioBytesWritten", BasicType::UINT64)
        ->addField(prefix + "batteryStatus", BasicType::UINT64)
        ->addField(prefix + "latCoord", BasicType::UINT64)
        ->addField(prefix + "longCoord", BasicType::UINT64);

    return schema;
}

RuntimeNesMetrics RuntimeNesMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    RuntimeNesMetrics output{};
    auto i = schema->getIndex(prefix);
    auto rnSchema = RuntimeNesMetrics::getSchema("");

    if (i >= schema->getSize()) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Prefix " + prefix + " could not be found in schema:\n" + schema->toString());
    }
    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Tuple size should be 1, but is " + std::to_string(buf.getNumberOfTuples()));
    }

    auto hasName = UtilityFunctions::endsWith(schema->fields[i]->getName(), rnSchema->get(0)->getName());
    auto hasLastField = UtilityFunctions::endsWith(schema->fields[i + rnSchema->getSize() - 1]->getName(), rnSchema->get(rnSchema->getSize())->getName());

    if (!hasName || !hasLastField) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Missing fields in schema.");
    }

    auto layout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.memoryUsageInBytes = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.cpuLoadInJiffies = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.blkioBytesRead = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.blkioBytesWritten = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.batteryStatus = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.latCoord = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.longCoord = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    return output;
}

web::json::value RuntimeNesMetrics::toJson() const {
    web::json::value metricsJson{};

    metricsJson["MemoryUsageInBytes"] = web::json::value::number(memoryUsageInBytes);
    metricsJson["CpuLoadInJiffies"] = web::json::value::number(cpuLoadInJiffies);
    metricsJson["BlkioBytesRead"] = web::json::value::number(blkioBytesRead);
    metricsJson["BlkioBytesWritten"] = web::json::value::number(blkioBytesWritten);
    metricsJson["BatteryStatus"] = web::json::value::number(batteryStatus);
    metricsJson["LatCoord"] = web::json::value::number(latCoord);
    metricsJson["LongCoord"] = web::json::value::number(longCoord);

    return metricsJson;
}

void writeToBuffer(const RuntimeNesMetrics& metric, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(RuntimeNesMetrics) < buf.getBufferSize(), "RuntimeNesMetrics: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metric, sizeof(RuntimeNesMetrics));
    buf.setNumberOfTuples(1);
}

}// namespace NES
