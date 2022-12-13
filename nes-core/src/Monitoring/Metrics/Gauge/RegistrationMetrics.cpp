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
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

RegistrationMetrics::RegistrationMetrics()
    : nodeId(0), totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(false),
      hasBattery(false) {
    NES_DEBUG("RegistrationMetrics: Default ctor");
}

RegistrationMetrics::RegistrationMetrics(bool isMoving, bool hasBattery)
    : nodeId(0), totalMemoryBytes(0), cpuCoreNum(0), totalCPUJiffies(0), cpuPeriodUS(0), cpuQuotaUS(0), isMoving(isMoving),
      hasBattery(hasBattery) {
    NES_DEBUG("RegistrationMetrics: Init with flag moving:" + std::to_string(isMoving)
              + ", hasBattery:" + std::to_string(hasBattery));
}

RegistrationMetrics::RegistrationMetrics(const SerializableRegistrationMetrics& metrics)
    : nodeId(0), totalMemoryBytes(metrics.totalmemorybytes()), cpuCoreNum(metrics.cpucorenum()),
      totalCPUJiffies(metrics.totalcpujiffies()), cpuPeriodUS(metrics.cpuperiodus()), cpuQuotaUS(metrics.cpuquotaus()),
      isMoving(metrics.ismoving()), hasBattery(metrics.hasbattery()) {
    NES_DEBUG("RegistrationMetrics: Creating from serializable object.");
}

NES::SchemaPtr RegistrationMetrics::getSchema(const std::string& prefix) {
    NES::SchemaPtr schema = NES::Schema::create(NES::Schema::ROW_LAYOUT)
                                ->addField(prefix + "node_id", BasicType::UINT64)

                                ->addField(prefix + "totalMemoryBytes", BasicType::UINT64)

                                ->addField(prefix + "cpuCoreNum", BasicType::UINT64)
                                ->addField(prefix + "totalCPUJiffies", BasicType::UINT64)
                                ->addField(prefix + "cpuPeriodUS", BasicType::INT64)
                                ->addField(prefix + "cpuQuotaUS", BasicType::INT64)

                                ->addField(prefix + "isMoving", BasicType::BOOLEAN)
                                ->addField(prefix + "hasBattery", BasicType::BOOLEAN);

    return schema;
}

void RegistrationMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(RegistrationMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = RegistrationMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "RegistrationMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(totalMemoryBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(cpuCoreNum);
    buffer[tupleIndex][cnt++].write<uint64_t>(totalCPUJiffies);
    buffer[tupleIndex][cnt++].write<uint64_t>(cpuPeriodUS);
    buffer[tupleIndex][cnt++].write<uint64_t>(cpuQuotaUS);
    buffer[tupleIndex][cnt++].write<uint64_t>(isMoving);
    buffer[tupleIndex][cnt++].write<uint64_t>(hasBattery);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void RegistrationMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(RegistrationMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);
    uint64_t cnt = 0;

    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    totalMemoryBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    cpuCoreNum = buffer[tupleIndex][cnt++].read<uint64_t>();
    totalCPUJiffies = buffer[tupleIndex][cnt++].read<uint64_t>();
    cpuPeriodUS = buffer[tupleIndex][cnt++].read<uint64_t>();
    cpuQuotaUS = buffer[tupleIndex][cnt++].read<uint64_t>();
    isMoving = buffer[tupleIndex][cnt++].read<bool>();
    hasBattery = buffer[tupleIndex][cnt++].read<bool>();
}

nlohmann::json RegistrationMetrics::toJson() const {
    nlohmann::json metricsJson{};
    metricsJson["NODE_ID"] = nodeId;
    metricsJson["TotalMemory"] = totalMemoryBytes;

    metricsJson["CpuCoreNum"] = cpuCoreNum;
    metricsJson["TotalCPUJiffies"] = totalCPUJiffies;
    metricsJson["CpuPeriodUS"] = cpuPeriodUS;
    metricsJson["CpuQuotaUS"] = cpuQuotaUS;

    metricsJson["IsMoving"] = isMoving;
    metricsJson["HasBattery"] = hasBattery;

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
    return nodeId == rhs.nodeId && totalMemoryBytes == rhs.totalMemoryBytes && cpuCoreNum == rhs.cpuCoreNum
        && totalCPUJiffies == rhs.totalCPUJiffies && cpuPeriodUS == rhs.cpuPeriodUS && cpuQuotaUS == rhs.cpuQuotaUS
        && isMoving == rhs.isMoving && hasBattery == rhs.hasBattery;
}

bool RegistrationMetrics::operator!=(const RegistrationMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(RegistrationMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const RegistrationMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring