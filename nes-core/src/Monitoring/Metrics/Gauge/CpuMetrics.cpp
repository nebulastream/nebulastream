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

#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr CpuMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "core_num", BasicType::UINT64)
                           ->addField(prefix + "user", BasicType::UINT64)
                           ->addField(prefix + "nice", BasicType::UINT64)
                           ->addField(prefix + "system", BasicType::UINT64)
                           ->addField(prefix + "idle", BasicType::UINT64)
                           ->addField(prefix + "iowait", BasicType::UINT64)
                           ->addField(prefix + "irq", BasicType::UINT64)
                           ->addField(prefix + "softirq", BasicType::UINT64)
                           ->addField(prefix + "steal", BasicType::UINT64)
                           ->addField(prefix + "guest", BasicType::UINT64)
                           ->addField(prefix + "guestnice", BasicType::UINT64);
    return schema;
}

void CpuMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t byteOffset) const {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(CpuMetrics) <= buf.getBufferSize(), "CpuMetrics: Content does not fit in TupleBuffer");
    memcpy(tbuffer + byteOffset, this, sizeof(CpuMetrics));
    buf.setNumberOfTuples(buf.getNumberOfTuples()+1);
}

void CpuMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    //get index where the schema is starting
    auto schema = getSchema("");
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());

    int cnt = 0;
    core_num = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    user = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    nice = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    system = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    idle = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    iowait = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    irq = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    softirq = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    steal = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    guest = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    guestnice = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
}

std::ostream& operator<<(std::ostream& os, const CpuMetrics& values) {
    os << "core_num: " << values.core_num << "user: " << values.user << " nice: " << values.nice << " system: " << values.system
       << " idle: " << values.idle << " iowait: " << values.iowait << " irq: " << values.irq << " softirq: " << values.softirq
       << " steal: " << values.steal << " guest: " << values.guest << " guestnice: " << values.guestnice;
    return os;
}

web::json::value CpuMetrics::toJson() const {
    web::json::value metricsJson{};
    metricsJson["CORE_NUM"] = web::json::value::number(core_num);
    metricsJson["USER"] = web::json::value::number(user);
    metricsJson["NICE"] = web::json::value::number(nice);
    metricsJson["SYSTEM"] = web::json::value::number(system);
    metricsJson["IDLE"] = web::json::value::number(idle);
    metricsJson["IOWAIT"] = web::json::value::number(iowait);
    metricsJson["IRQ"] = web::json::value::number(irq);
    metricsJson["SOFTIRQ"] = web::json::value::number(softirq);
    metricsJson["STEAL"] = web::json::value::number(steal);
    metricsJson["GUEST"] = web::json::value::number(guest);
    metricsJson["GUESTNICE"] = web::json::value::number(guestnice);

    return metricsJson;
}

bool CpuMetrics::operator==(const CpuMetrics& rhs) const {
    return core_num == rhs.core_num && user == rhs.user && nice == rhs.nice && system == rhs.system && idle == rhs.idle
        && iowait == rhs.iowait && irq == rhs.irq && softirq == rhs.softirq && steal == rhs.steal && guest == rhs.guest
        && guestnice == rhs.guestnice;
}

bool CpuMetrics::operator!=(const CpuMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    metrics.writeToBuffer(buf, byteOffset);
}

void readFromBuffer(CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    metrics.readFromBuffer(buf, byteOffset);
}

}// namespace NES
