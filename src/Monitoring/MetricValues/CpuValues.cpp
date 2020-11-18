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
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

SchemaPtr CpuValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "USER", BasicType::UINT64)
                           ->addField(prefix + "NICE", BasicType::UINT64)
                           ->addField(prefix + "SYSTEM", BasicType::UINT64)
                           ->addField(prefix + "IDLE", BasicType::UINT64)
                           ->addField(prefix + "IOWAIT", BasicType::UINT64)
                           ->addField(prefix + "IRQ", BasicType::UINT64)
                           ->addField(prefix + "SOFTIRQ", BasicType::UINT64)
                           ->addField(prefix + "STEAL", BasicType::UINT64)
                           ->addField(prefix + "GUEST", BasicType::UINT64)
                           ->addField(prefix + "GUESTNICE", BasicType::UINT64);
    return schema;
}

CpuValues CpuValues::fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    CpuValues output{};
    //get index where the schema for CpuValues is starting
    auto i = schema->getIndex(prefix + "USER");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->name, "USER")
        && UtilityFunctions::endsWith(schema->fields[i + 9]->name, "GUESTNICE")) {
        NES_DEBUG("CpuValues: Index found for " + prefix + "USER" + " at " + std::to_string(i));
        auto layout = createRowLayout(schema);
        //set the values to the output object
        output.USER = layout->getValueField<uint64_t>(0, i)->read(buf);
        output.NICE = layout->getValueField<uint64_t>(0, i + 1)->read(buf);
        output.SYSTEM = layout->getValueField<uint64_t>(0, i + 2)->read(buf);
        output.IDLE = layout->getValueField<uint64_t>(0, i + 3)->read(buf);
        output.IOWAIT = layout->getValueField<uint64_t>(0, i + 4)->read(buf);
        output.IRQ = layout->getValueField<uint64_t>(0, i + 5)->read(buf);
        output.SOFTIRQ = layout->getValueField<uint64_t>(0, i + 6)->read(buf);
        output.STEAL = layout->getValueField<uint64_t>(0, i + 7)->read(buf);
        output.GUEST = layout->getValueField<uint64_t>(0, i + 8)->read(buf);
        output.GUESTNICE = layout->getValueField<uint64_t>(0, i + 9)->read(buf);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuValues: Metrics could not be parsed from schema with prefix " + prefix + ":\n"
                                + schema->toString());
    }
    return output;
}

void serialize(const CpuValues& metric, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    auto schemaSize = schema->getSize();
    schema->copyFields(CpuValues::getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, schemaSize)->write(buf, metric.USER);
    layout->getValueField<uint64_t>(0, schemaSize + 1)->write(buf, metric.NICE);
    layout->getValueField<uint64_t>(0, schemaSize + 2)->write(buf, metric.SYSTEM);
    layout->getValueField<uint64_t>(0, schemaSize + 3)->write(buf, metric.IDLE);
    layout->getValueField<uint64_t>(0, schemaSize + 4)->write(buf, metric.IOWAIT);
    layout->getValueField<uint64_t>(0, schemaSize + 5)->write(buf, metric.IRQ);
    layout->getValueField<uint64_t>(0, schemaSize + 6)->write(buf, metric.SOFTIRQ);
    layout->getValueField<uint64_t>(0, schemaSize + 7)->write(buf, metric.STEAL);
    layout->getValueField<uint64_t>(0, schemaSize + 8)->write(buf, metric.GUEST);
    layout->getValueField<uint64_t>(0, schemaSize + 9)->write(buf, metric.GUESTNICE);
    buf.setNumberOfTuples(1);
}

}// namespace NES
