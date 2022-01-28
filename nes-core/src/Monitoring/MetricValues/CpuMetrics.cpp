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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cpprest/json.h>
#include <cstring>

namespace NES {

CpuMetrics::CpuMetrics(CpuValues total, unsigned int size, std::vector<CpuValues>&& arr) : total(total), numCores(size) {
    if (numCores > 0) {
        cpuValues = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Object cannot be allocated with less than 0 cores.");
    }
    NES_DEBUG("CpuMetrics: Allocating memory for " + std::to_string(numCores) + " metrics.");
}

uint16_t CpuMetrics::getNumCores() const { return numCores; }

CpuValues CpuMetrics::getValues(const unsigned int cpuCore) const { return cpuValues.at(cpuCore); }

CpuValues CpuMetrics::getTotal() const { return total; }

CpuMetrics CpuMetrics::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    //get index where the schema for CpuMetrics is starting
    auto idx = schema->getIndex(prefix + "CORE_NO");

    if (idx < schema->getSize() && buf.getNumberOfTuples() == 1 && Util::endsWith(schema->fields[idx]->getName(), "CORE_NO")) {
        //if schema contains cpuMetrics parse the wrapper object
        auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());
        auto numCores = Runtime::MemoryLayouts::RowLayoutField<uint16_t, true>::create(idx, layout, buf)[0];

        auto cpu = std::vector<CpuValues>(numCores);
        auto totalCpu = CpuValues::fromBuffer(schema, buf, prefix + "CPU[TOTAL]_");

        for (int n = 0; n < numCores; n++) {
            //for each core parse the according CpuValues
            cpu[n] = CpuValues::fromBuffer(schema, buf, prefix + "CPU[" + std::to_string(n + 1) + "]_");
        }
        return CpuMetrics{totalCpu, numCores, std::move(cpu)};
    }
    NES_THROW_RUNTIME_ERROR("CpuMetrics: Prefix " + prefix + " could not be parsed from schema " + schema->toString());
    throw 0;// just to make the compiler happy
}

web::json::value CpuMetrics::toJson() {
    web::json::value metricsJson{};

    metricsJson["NUM_CORES"] = web::json::value::number(numCores);
    metricsJson["TOTAL"] = total.toJson();

    for (int n = 0; n < numCores; n++) {
        metricsJson["CORE_" + std::to_string(n)] = cpuValues[n].toJson();
    }

    return metricsJson;
}

void writeToBuffer(const CpuMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    uint64_t totalSize = byteOffset + sizeof(uint16_t) + sizeof(CpuValues) * (metrics.getNumCores() + 1);
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "CpuMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    auto coreNum = metrics.getNumCores();
    memcpy(tbuffer + byteOffset, &coreNum, sizeof(uint16_t));
    byteOffset += sizeof(uint16_t);

    writeToBuffer(metrics.getTotal(), buf, byteOffset);
    byteOffset += sizeof(CpuValues);

    for (unsigned int i = 0; i < coreNum; i++) {
        writeToBuffer(metrics.getValues(i), buf, byteOffset);
        byteOffset += sizeof(CpuValues);
    }

    buf.setNumberOfTuples(1);
}

SchemaPtr getSchema(const CpuMetrics& metrics, const std::string& prefix) {
    auto schema = Schema::create();
    schema->addField(prefix + "CORE_NO", BasicType::UINT16);

    //add schema for total
    schema->copyFields(CpuValues::getSchema(prefix + "CPU[TOTAL]_"));

    //add schema for each core
    for (int i = 0; i < metrics.getNumCores(); i++) {
        auto corePrefix = prefix + "CPU[" + std::to_string(i + 1) + "]_";
        schema->copyFields(CpuValues::getSchema(corePrefix));
    }

    return schema;
}

bool CpuMetrics::operator==(const CpuMetrics& rhs) const {
    if (cpuValues.size() != rhs.cpuValues.size()) {
        return false;
    }

    for (auto i = static_cast<decltype(cpuValues)::size_type>(0); i < cpuValues.size(); ++i) {
        if (cpuValues[i] != rhs.cpuValues[i]) {
            return false;
        }
    }

    return total == rhs.total && numCores == rhs.numCores;
}

bool CpuMetrics::operator!=(const CpuMetrics& rhs) const { return !(rhs == *this); }

}// namespace NES