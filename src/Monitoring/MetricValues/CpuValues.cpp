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

#include <Monitoring/MetricValues/CpuValues.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <NodeEngine/MemoryLayout/DynamicRowLayoutField.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr CpuValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
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

CpuValues CpuValues::fromBuffer(const SchemaPtr& schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    CpuValues output{};
    //get index where the schema for CpuValues is starting
    auto i = schema->getIndex(prefix + "user");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->getName(), "user")
        && UtilityFunctions::endsWith(schema->fields[i + 9]->getName(), "guestnice")) {
        NES_DEBUG("CpuValues: Index found for " + prefix + "user" + " at " + std::to_string(i));

        auto layout = NodeEngine::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
        auto bindedRowLayout = layout->bind(buf);

        output.user = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 0, bindedRowLayout)[0];
        output.nice = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 1, bindedRowLayout)[0];
        output.system = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 2, bindedRowLayout)[0];
        output.idle = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 3, bindedRowLayout)[0];
        output.iowait = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 4, bindedRowLayout)[0];
        output.irq = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 5, bindedRowLayout)[0];
        output.softirq =
            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 6, bindedRowLayout)[0];
        output.steal = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 7, bindedRowLayout)[0];
        output.guest = NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 8, bindedRowLayout)[0];
        output.guestnice =
            NodeEngine::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i + 9, bindedRowLayout)[0];

    } else {
        NES_THROW_RUNTIME_ERROR("CpuValues: Metrics could not be parsed from schema with prefix " + prefix + ":\n"
                                + schema->toString());
    }
    return output;
}

std::ostream& operator<<(std::ostream& os, const CpuValues& values) {
    os << "user: " << values.user << " nice: " << values.nice << " system: " << values.system << " idle: " << values.idle
       << " iowait: " << values.iowait << " irq: " << values.irq << " softirq: " << values.softirq << " steal: " << values.steal
       << " guest: " << values.guest << " guestnice: " << values.guestnice;
    return os;
}

web::json::value CpuValues::toJson() {
    web::json::value metricsJson{};

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

void writeToBuffer(const CpuValues& metrics, NodeEngine::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(CpuValues) < buf.getBufferSize(), "CpuValues: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metrics, sizeof(CpuValues));
    buf.setNumberOfTuples(1);
}

bool CpuValues::operator==(const CpuValues& rhs) const {
    return user == rhs.user && nice == rhs.nice && system == rhs.system && idle == rhs.idle && iowait == rhs.iowait
        && irq == rhs.irq && softirq == rhs.softirq && steal == rhs.steal && guest == rhs.guest && guestnice == rhs.guestnice;
}
bool CpuValues::operator!=(const CpuValues& rhs) const { return !(rhs == *this); }

}// namespace NES
