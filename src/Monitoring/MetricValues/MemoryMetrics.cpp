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
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

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

void writeToBuffer(const MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(MemoryMetrics) <= buf.getBufferSize(), "MemoryMetrics: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metrics, sizeof(MemoryMetrics));
    buf.setNumberOfTuples(1);
}

MemoryMetrics MemoryMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    MemoryMetrics output{};
    auto i = schema->getIndex(prefix + "TOTAL_RAM");

    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Tuple size should be 1, but is larger "
                                + std::to_string(buf.getNumberOfTuples()));
    }

    if (!MetricUtils::validateFieldsInSchema(MemoryMetrics::getSchema(""), schema, i)) {
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Incomplete number of fields in schema.");
    }

    //if buffer contains memory metric information read the values from each buffer and assign them to the output wrapper object
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.TOTAL_RAM = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.TOTAL_SWAP = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.FREE_RAM = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.SHARED_RAM = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.BUFFER_RAM = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.FREE_SWAP = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.TOTAL_HIGH = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.FREE_HIGH = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.PROCS = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.MEM_UNIT = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.LOADS_1MIN = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.LOADS_5MIN = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.LOADS_15MIN = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    return output;
}

SchemaPtr getSchema(const MemoryMetrics&, const std::string& prefix) { return MemoryMetrics::getSchema(prefix); }

bool MemoryMetrics::operator==(const MemoryMetrics& rhs) const {
    return TOTAL_RAM == rhs.TOTAL_RAM && TOTAL_SWAP == rhs.TOTAL_SWAP && FREE_RAM == rhs.FREE_RAM && SHARED_RAM == rhs.SHARED_RAM
        && BUFFER_RAM == rhs.BUFFER_RAM && FREE_SWAP == rhs.FREE_SWAP && TOTAL_HIGH == rhs.TOTAL_HIGH
        && FREE_HIGH == rhs.FREE_HIGH && PROCS == rhs.PROCS && MEM_UNIT == rhs.MEM_UNIT && LOADS_1MIN == rhs.LOADS_1MIN
        && LOADS_5MIN == rhs.LOADS_5MIN && LOADS_15MIN == rhs.LOADS_15MIN;
}

bool MemoryMetrics::operator!=(const MemoryMetrics& rhs) const { return !(rhs == *this); }

web::json::value MemoryMetrics::toJson() const {
    web::json::value metricsJson{};
    metricsJson["TOTAL_RAM"] = web::json::value::number(TOTAL_RAM);
    metricsJson["TOTAL_SWAP"] = web::json::value::number(TOTAL_SWAP);
    metricsJson["FREE_RAM"] = web::json::value::number(FREE_RAM);
    metricsJson["SHARED_RAM"] = web::json::value::number(SHARED_RAM);
    metricsJson["BUFFER_RAM"] = web::json::value::number(BUFFER_RAM);
    metricsJson["FREE_SWAP"] = web::json::value::number(FREE_SWAP);
    metricsJson["TOTAL_HIGH"] = web::json::value::number(TOTAL_HIGH);
    metricsJson["FREE_HIGH"] = web::json::value::number(FREE_HIGH);
    metricsJson["PROCS"] = web::json::value::number(PROCS);
    metricsJson["MEM_UNIT"] = web::json::value::number(MEM_UNIT);
    metricsJson["LOADS_1MIN"] = web::json::value::number(LOADS_1MIN);
    metricsJson["LOADS_5MIN"] = web::json::value::number(LOADS_5MIN);
    metricsJson["LOADS_15MIN"] = web::json::value::number(LOADS_15MIN);
    return metricsJson;
}

}// namespace NES