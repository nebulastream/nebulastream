#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <cstring>

namespace NES {

CpuMetrics::CpuMetrics(CpuValues total, unsigned int size, std::unique_ptr<CpuValues[]> arr) : total(total), cpuNo(size) {
    if (cpuNo > 0) {
        ptr = std::move(arr);
    } else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Object cannot be allocated with less than 0 cores.");
    }
    NES_DEBUG("CpuMetrics: Allocating memory for " + std::to_string(cpuNo) + " metrics.");
}

CpuMetrics::~CpuMetrics() {
    ptr.reset();
    NES_DEBUG("CpuMetrics: Freeing memory for metrics.");
}

// Implementation of [] operator.  This function must return a
// reference as array element can be put on left side
CpuValues& CpuMetrics::operator[](unsigned int index) {
    if (index >= cpuNo) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(index) + ">=" + std::to_string(cpuNo));
    }
    return ptr[index];
}

uint16_t CpuMetrics::getCpuNo() const {
    return cpuNo;
}

CpuValues CpuMetrics::getValues(const unsigned int cpuCore) const {
    return ptr[cpuCore];
}

CpuValues CpuMetrics::getTotal() const {
    return total;
}

std::pair<std::shared_ptr<Schema>, TupleBuffer> CpuMetrics::serialize(BufferManagerPtr bm) {
    SchemaPtr outputSchema = Schema::create();
    outputSchema->addField("CPU_NO", BasicType::UINT16);
    TupleBuffer outputBuf = bm->getBufferBlocking();
    outputBuf.setNumberOfTuples(1);

    auto layout = createRowLayout(outputSchema);
    layout->getValueField<uint64_t>(0, 0)->write(outputBuf, cpuNo);
    total.serialize(outputSchema, outputBuf);

    return std::make_pair(outputSchema, outputBuf);
}

}// namespace NES