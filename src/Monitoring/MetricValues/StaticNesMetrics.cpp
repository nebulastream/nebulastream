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

#include <Monitoring/MetricValues/StaticNesMetrics.hpp>

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

StaticNesMetrics::StaticNesMetrics()
    : totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(false), hasBattery(false) {
    NES_DEBUG("StaticNesMetrics: Default ctor");
}

StaticNesMetrics::StaticNesMetrics(bool isMoving, bool hasBattery)
    : totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(isMoving),
      hasBattery(hasBattery) {
    NES_DEBUG("StaticNesMetrics: Init with flag moving:" + std::to_string(isMoving)
              + ", hasBattery:" + std::to_string(hasBattery));
}

SchemaPtr StaticNesMetrics::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "totalMemoryBytes", BasicType::UINT64)

                           ->addField(prefix + "cpuCoreNum", BasicType::UINT16)
                           ->addField(prefix + "totalCPUJiffies", BasicType::UINT64)
                           ->addField(prefix + "cpuPeriodUS", BasicType::INT64)
                           ->addField(prefix + "cpuQuotaUS", BasicType::INT64)

                           ->addField(prefix + "isMoving", BasicType::BOOLEAN)
                           ->addField(prefix + "hasBattery", BasicType::BOOLEAN);

    return schema;
}

StaticNesMetrics StaticNesMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    StaticNesMetrics output{};
    auto i = schema->getIndex(prefix);
    auto rnSchema = StaticNesMetrics::getSchema("");

    if (i >= schema->getSize()) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Prefix " + prefix + " could not be found in schema:\n" + schema->toString());
    }
    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Tuple size should be 1, but is " + std::to_string(buf.getNumberOfTuples()));
    }

    auto hasName = UtilityFunctions::endsWith(schema->fields[i]->getName(), rnSchema->get(0)->getName());
    auto hasLastField = UtilityFunctions::endsWith(schema->fields[i + rnSchema->getSize() - 1]->getName(),
                                                   rnSchema->get(rnSchema->getSize() - 1)->getName());

    if (!hasName || !hasLastField) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Missing fields in schema.");
    }

    auto layout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.totalMemoryBytes =
        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    output.cpuCoreNum = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint16_t, true>::create(i++, bindedRowLayout)[0];
    output.totalCPUJiffies = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.cpuPeriodUS = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(i++, bindedRowLayout)[0];
    output.cpuQuotaUS = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, true>::create(i++, bindedRowLayout)[0];

    output.isMoving = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<bool, true>::create(i++, bindedRowLayout)[0];
    output.hasBattery = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<bool, true>::create(i++, bindedRowLayout)[0];

    return output;
}

web::json::value StaticNesMetrics::toJson() const {
    web::json::value metricsJson{};

    metricsJson["TotalMemory"] = web::json::value::number(totalMemoryBytes);

    metricsJson["CpuCoreNum"] = web::json::value::number(cpuCoreNum);
    metricsJson["TotalCPUJiffies"] = web::json::value::number(totalCPUJiffies);
    metricsJson["CpuPeriodUS"] = web::json::value::number(cpuPeriodUS);
    metricsJson["CpuQuotaUS"] = web::json::value::number(cpuQuotaUS);

    metricsJson["IsMoving"] = web::json::value::number(isMoving);
    metricsJson["HasBattery"] = web::json::value::number(hasBattery);

    return metricsJson;
}

bool StaticNesMetrics::operator==(const StaticNesMetrics& rhs) const {
    return totalMemoryBytes == rhs.totalMemoryBytes && cpuCoreNum == rhs.cpuCoreNum && totalCPUJiffies == rhs.totalCPUJiffies
        && cpuPeriodUS == rhs.cpuPeriodUS && cpuQuotaUS == rhs.cpuQuotaUS && isMoving == rhs.isMoving
        && hasBattery == rhs.hasBattery;
}
bool StaticNesMetrics::operator!=(const StaticNesMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const StaticNesMetrics& metric, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(StaticNesMetrics) < buf.getBufferSize(),
               "RuntimeNesMetrics: Content does not fit in TupleBuffer");
    NES_ASSERT(sizeof(StaticNesMetrics) == StaticNesMetrics::getSchema("")->getSchemaSizeInBytes(),
               sizeof(StaticNesMetrics) << "!=" << StaticNesMetrics::getSchema("")->getSchemaSizeInBytes());

    memcpy(tbuffer + byteOffset, &metric, sizeof(StaticNesMetrics));
    buf.setNumberOfTuples(1);
}

SchemaPtr getSchema(const StaticNesMetrics&, const std::string& prefix) { return StaticNesMetrics::getSchema(prefix); }

}// namespace NES