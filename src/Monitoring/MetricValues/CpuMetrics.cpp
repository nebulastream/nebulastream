#include <API/Schema.hpp>
#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

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

void serialize(CpuMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
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