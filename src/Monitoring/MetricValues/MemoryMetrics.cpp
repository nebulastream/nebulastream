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

#include <Monitoring/MetricValues/MemoryMetrics.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

SchemaPtr MemoryMetrics::getSchema(const std::string& prefix) {
    auto schema = Schema::create()
                      ->addField(prefix + "TOTAL_RAM", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_SWAP", BasicType::UINT64)
                      ->addField(prefix + "FREE_RAM", BasicType::UINT64)
                      ->addField(prefix + "SHARED_RAM", BasicType::UINT64)
                      ->addField(prefix + "BUFFER_RAM", BasicType::UINT64)
                      ->addField(prefix + "FREE_SWAP", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_HIGH", BasicType::UINT64)
                      ->addField(prefix + "FREE_HIGH", BasicType::UINT64)
                      ->addField(prefix + "PROCS", BasicType::UINT64)
                      ->addField(prefix + "MEM_UNIT", BasicType::UINT64)
                      ->addField(prefix + "LOADS_1MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_5MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_15MIN", BasicType::UINT64);
    return schema;
}

void serialize(const MemoryMetrics& metric, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    buf.setNumberOfTuples(1);

    auto noFields = schema->getSize();
    schema->copyFields(MemoryMetrics::getSchema(prefix));

    auto layout = NodeEngine::createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.TOTAL_RAM);
    layout->getValueField<uint64_t>(0, noFields + 1)->write(buf, metric.TOTAL_SWAP);
    layout->getValueField<uint64_t>(0, noFields + 2)->write(buf, metric.FREE_RAM);
    layout->getValueField<uint64_t>(0, noFields + 3)->write(buf, metric.SHARED_RAM);
    layout->getValueField<uint64_t>(0, noFields + 4)->write(buf, metric.BUFFER_RAM);
    layout->getValueField<uint64_t>(0, noFields + 5)->write(buf, metric.FREE_SWAP);
    layout->getValueField<uint64_t>(0, noFields + 6)->write(buf, metric.TOTAL_HIGH);
    layout->getValueField<uint64_t>(0, noFields + 7)->write(buf, metric.FREE_HIGH);
    layout->getValueField<uint64_t>(0, noFields + 8)->write(buf, metric.PROCS);
    layout->getValueField<uint64_t>(0, noFields + 9)->write(buf, metric.MEM_UNIT);
    layout->getValueField<uint64_t>(0, noFields + 10)->write(buf, metric.LOADS_1MIN);
    layout->getValueField<uint64_t>(0, noFields + 11)->write(buf, metric.LOADS_5MIN);
    layout->getValueField<uint64_t>(0, noFields + 12)->write(buf, metric.LOADS_15MIN);
}

MemoryMetrics MemoryMetrics::fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    MemoryMetrics output{};
    //get index where the schema for MemoryMetrics is starting
    auto i = schema->getIndex(prefix + "TOTAL_RAM");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->name, "TOTAL_RAM")
        && UtilityFunctions::endsWith(schema->fields[i + 12]->name, "LOADS_15MIN")) {
        //if buffer contains memory metric information read the values from each buffer and assign them to the output wrapper object
        auto layout = NodeEngine::createRowLayout(schema);
        output.TOTAL_RAM = layout->getValueField<uint64_t>(0, i)->read(buf);
        output.TOTAL_SWAP = layout->getValueField<uint64_t>(0, i + 1)->read(buf);
        output.FREE_RAM = layout->getValueField<uint64_t>(0, i + 2)->read(buf);
        output.SHARED_RAM = layout->getValueField<uint64_t>(0, i + 3)->read(buf);
        output.BUFFER_RAM = layout->getValueField<uint64_t>(0, i + 4)->read(buf);
        output.FREE_SWAP = layout->getValueField<uint64_t>(0, i + 5)->read(buf);
        output.TOTAL_HIGH = layout->getValueField<uint64_t>(0, i + 6)->read(buf);
        output.FREE_HIGH = layout->getValueField<uint64_t>(0, i + 7)->read(buf);
        output.PROCS = layout->getValueField<uint64_t>(0, i + 8)->read(buf);
        output.MEM_UNIT = layout->getValueField<uint64_t>(0, i + 9)->read(buf);
        output.LOADS_1MIN = layout->getValueField<uint64_t>(0, i + 10)->read(buf);
        output.LOADS_5MIN = layout->getValueField<uint64_t>(0, i + 11)->read(buf);
        output.LOADS_15MIN = layout->getValueField<uint64_t>(0, i + 12)->read(buf);
    } else {
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Metrics could not be parsed from schema " + schema->toString());
    }
    return output;
}

bool MemoryMetrics::operator==(const MemoryMetrics& rhs) const {
    return TOTAL_RAM == rhs.TOTAL_RAM && TOTAL_SWAP == rhs.TOTAL_SWAP && FREE_RAM == rhs.FREE_RAM && SHARED_RAM == rhs.SHARED_RAM
        && BUFFER_RAM == rhs.BUFFER_RAM && FREE_SWAP == rhs.FREE_SWAP && TOTAL_HIGH == rhs.TOTAL_HIGH
        && FREE_HIGH == rhs.FREE_HIGH && PROCS == rhs.PROCS && MEM_UNIT == rhs.MEM_UNIT && LOADS_1MIN == rhs.LOADS_1MIN
        && LOADS_5MIN == rhs.LOADS_5MIN && LOADS_15MIN == rhs.LOADS_15MIN;
}

bool MemoryMetrics::operator!=(const MemoryMetrics& rhs) const { return !(rhs == *this); }

}// namespace NES