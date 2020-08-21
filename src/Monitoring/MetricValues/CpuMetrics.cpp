#include <API/Schema.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/Metrics/MetricDefinition.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

CpuMetrics::CpuMetrics(CpuValues total, unsigned int size, std::unique_ptr<CpuValues[]> arr) : total(total), numCores(size) {
    if (numCores > 0) {
        ptr = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Object cannot be allocated with less than 0 cores.");
    }
    NES_DEBUG("CpuMetrics: Allocating memory for " + std::to_string(numCores) + " metrics.");
}

CpuMetrics::~CpuMetrics() {
    ptr.reset();
    NES_DEBUG("CpuMetrics: Freeing memory for metrics.");
}

// Implementation of [] operator.  This function must return a reference as array element can be put on left side
CpuValues& CpuMetrics::operator[](unsigned int index) {
    if (index >= numCores) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(index) + ">=" + std::to_string(numCores));
    }
    return ptr[index];
}

uint16_t CpuMetrics::getNumCores() const {
    return numCores;
}

CpuValues CpuMetrics::getValues(const unsigned int cpuCore) const {
    return ptr[cpuCore];
}

CpuValues CpuMetrics::getTotal() const {
    return total;
}

CpuMetrics CpuMetrics::fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto i = schema->getIndex(prefix+"CORE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->name, "CORE_NO")) {
        auto layout = createRowLayout(schema);
        auto numCores = layout->getValueField<uint16_t>(0, i)->read(buf);
        auto cpu = std::unique_ptr<CpuValues[]>(new CpuValues[numCores]);
        auto totalCpu = CpuValues::fromBuffer(schema, buf, prefix + "CPU[TOTAL]_");

        for (int n=0; n++; n< numCores) {
            cpu[n] = CpuValues::fromBuffer(schema, buf, prefix + "CPU[" + std::to_string(n+1) + "]_" );
        }
        return CpuMetrics{totalCpu, numCores, std::move(cpu)};
    }
    else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Metrics could not be parsed from schema " + schema->toString());
    }
}

void serialize(CpuMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def, const std::string& prefix) {
    if (def.cpuMetrics.find(prefix) != def.cpuMetrics.end()) {
        //element is already in MetricDefinition
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Error during serialize(..): Metric with " +
            prefix + " is already in MetricDefinition.");
    }
    else {
        def.cpuMetrics.insert(prefix);

        // extend the schema with the core number
        auto noFields = schema->getSize();
        schema->addField(prefix + "CORE_NO", BasicType::UINT16);
        // the buffer contains only one metric tuple
        buf.setNumberOfTuples(1);

        auto layout = createRowLayout(schema);
        layout->getValueField<uint16_t>(0, noFields)->write(buf, metrics.getNumCores());
        // call serialize for the total metrics over all cores
        serialize(metrics.getTotal(), schema, buf, def, prefix + "CPU[TOTAL]_");

        for (int i=0; i< metrics.getNumCores(); i++) {
            //call serialize method for the metrics for each cpu
            serialize(metrics.getValues(i), schema, buf, def, prefix + "CPU[" + std::to_string(i+1) + "]_");
        }
    }
}

}// namespace NES