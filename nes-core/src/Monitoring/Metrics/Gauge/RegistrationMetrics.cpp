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

#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>

#include <API/Schema.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

RegistrationMetrics::RegistrationMetrics()
    : totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(false), hasBattery(false) {
    NES_DEBUG("RegistrationMetrics: Default ctor");
}

RegistrationMetrics::RegistrationMetrics(bool isMoving, bool hasBattery)
    : totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(isMoving),
      hasBattery(hasBattery) {
    NES_DEBUG("RegistrationMetrics: Init with flag moving:" + std::to_string(isMoving)
              + ", hasBattery:" + std::to_string(hasBattery));
}

RegistrationMetrics::RegistrationMetrics(const SerializableRegistrationMetrics& metrics)
    : totalMemoryBytes(metrics.totalmemorybytes()), cpuCoreNum(metrics.cpucorenum()), totalCPUJiffies(metrics.totalcpujiffies()),
      cpuPeriodUS(metrics.cpuperiodus()), cpuQuotaUS(metrics.cpuquotaus()), isMoving(metrics.ismoving()),
      hasBattery(metrics.hasbattery()) {
    NES_DEBUG("RegistrationMetrics: Creating from serializable object.");
}

SchemaPtr RegistrationMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "totalMemoryBytes", BasicType::UINT64)

                           ->addField(prefix + "cpuCoreNum", BasicType::UINT32)
                           ->addField(prefix + "totalCPUJiffies", BasicType::UINT64)
                           ->addField(prefix + "cpuPeriodUS", BasicType::INT64)
                           ->addField(prefix + "cpuQuotaUS", BasicType::INT64)

                           ->addField(prefix + "isMoving", BasicType::BOOLEAN)
                           ->addField(prefix + "hasBattery", BasicType::BOOLEAN);

    return schema;
}

void RegistrationMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    auto byteOffset = tupleIndex * sizeof(RegistrationMetrics);
    NES_ASSERT(byteOffset + sizeof(RegistrationMetrics) <= buf.getBufferSize(),
               "RuntimeMetrics: Content does not fit in TupleBuffer");
    NES_ASSERT(sizeof(RegistrationMetrics) == RegistrationMetrics::getSchema("")->getSchemaSizeInBytes(),
               sizeof(RegistrationMetrics) << "!=" << RegistrationMetrics::getSchema("")->getSchemaSizeInBytes());

    memcpy(tbuffer + byteOffset, this, sizeof(RegistrationMetrics));
    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void RegistrationMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    //get index where the schema is starting
    auto schema = getSchema("");
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
    int cnt = 0;

    totalMemoryBytes = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];

    cpuCoreNum = Runtime::MemoryLayouts::RowLayoutField<uint16_t, true>::create(cnt++, layout, buf)[tupleIndex];
    totalCPUJiffies = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    cpuPeriodUS = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(cnt++, layout, buf)[tupleIndex];
    cpuQuotaUS = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(cnt++, layout, buf)[tupleIndex];

    isMoving = Runtime::MemoryLayouts::RowLayoutField<bool, true>::create(cnt++, layout, buf)[tupleIndex];
    hasBattery = Runtime::MemoryLayouts::RowLayoutField<bool, true>::create(cnt++, layout, buf)[tupleIndex];
}

web::json::value RegistrationMetrics::toJson() const {
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

SerializableRegistrationMetricsPtr RegistrationMetrics::serialize() const {
    auto output = std::make_shared<SerializableRegistrationMetrics>();
    output->set_ismoving(isMoving);
    output->set_hasbattery(hasBattery);
    output->set_totalmemorybytes(totalMemoryBytes);
    output->set_totalcpujiffies(totalCPUJiffies);
    output->set_cpucorenum(cpuCoreNum);
    output->set_cpuperiodus(cpuPeriodUS);
    output->set_cpuquotaus(cpuQuotaUS);
    return output;
}

bool RegistrationMetrics::operator==(const RegistrationMetrics& rhs) const {
    return totalMemoryBytes == rhs.totalMemoryBytes && cpuCoreNum == rhs.cpuCoreNum && totalCPUJiffies == rhs.totalCPUJiffies
        && cpuPeriodUS == rhs.cpuPeriodUS && cpuQuotaUS == rhs.cpuQuotaUS && isMoving == rhs.isMoving
        && hasBattery == rhs.hasBattery;
}

bool RegistrationMetrics::operator!=(const RegistrationMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

}// namespace NES