#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Util/Logger.hpp>

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

unsigned int CpuMetrics::size() const {
    return cpuNo;
}

CpuValues CpuMetrics::getValues(const unsigned int cpuCore) const {
    return ptr[cpuCore];
}

CpuValues CpuMetrics::getTotal() const {
    return total;
}

}// namespace NES