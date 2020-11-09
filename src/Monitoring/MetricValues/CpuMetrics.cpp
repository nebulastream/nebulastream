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

#include <API/Schema.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

CpuMetrics::CpuMetrics(CpuValues total, unsigned int size, std::vector<CpuValues>&& arr) : total(total), numCores(size) {
    if (numCores > 0) {
        cpuValues = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Object cannot be allocated with less than 0 cores.");
    }
    NES_DEBUG("CpuMetrics: Allocating memory for " + std::to_string(numCores) + " metrics.");
}

CpuMetrics::~CpuMetrics() {
    NES_DEBUG("CpuMetrics: Freeing memory for metrics.");
    cpuValues.clear();
}

uint16_t CpuMetrics::getNumCores() const {
    return numCores;
}

CpuValues CpuMetrics::getValues(const unsigned int cpuCore) const {
    return cpuValues.at(cpuCore);
}

CpuValues CpuMetrics::getTotal() const {
    return total;
}

CpuMetrics CpuMetrics::fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    //get index where the schema for CpuMetrics is starting
    auto idx = schema->getIndex(prefix + "CORE_NO");

    if (idx < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[idx]->name, "CORE_NO")) {
        //if schema contains cpuMetrics parse the wrapper object
        auto layout = createRowLayout(schema);
        auto numCores = layout->getValueField<uint16_t>(0, idx)->read(buf);
        auto cpu = std::vector<CpuValues>(numCores);
        auto totalCpu = CpuValues::fromBuffer(schema, buf, prefix + "CPU[TOTAL]_");

        for (int n = 0; n < numCores; n++) {
            //for each core parse the according CpuValues
            cpu[n] = CpuValues::fromBuffer(schema, buf, prefix + "CPU[" + std::to_string(n + 1) + "]_");
        }
        return CpuMetrics{totalCpu, numCores, std::move(cpu)};
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Prefix " + prefix + " could not be parsed from schema " + schema->toString());
    }
}

void serialize(const CpuMetrics& metrics, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    // extend the schema with the core number
    auto noFields = schema->getSize();
    schema->addField(prefix + "CORE_NO", BasicType::UINT16);
    // the buffer contains only one metric tuple
    buf.setNumberOfTuples(1);

    auto layout = createRowLayout(schema);
    layout->getValueField<uint16_t>(0, noFields)->write(buf, metrics.getNumCores());
    // call serialize for the total metrics over all cores
    serialize(metrics.getTotal(), schema, buf, prefix + "CPU[TOTAL]_");

    for (int i = 0; i < metrics.getNumCores(); i++) {
        //call serialize method for the metrics for each cpu
        serialize(metrics.getValues(i), schema, buf, prefix + "CPU[" + std::to_string(i + 1) + "]_");
    }
}

}// namespace NES