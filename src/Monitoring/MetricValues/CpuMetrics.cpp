#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Util/Logger.hpp>

namespace NES {
CpuMetrics::CpuMetrics(const unsigned int cpuNo): cpuNo(cpuNo) {
    ptr = nullptr;
    if (cpuNo > 0) {
        ptr = new CpuValues[cpuNo];
    }
}

// Implementation of [] operator.  This function must return a
// reference as array element can be put on left side
CpuValues& CpuMetrics::operator[](unsigned int index)
{
    if (index >= cpuNo) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(index) + ">=" + std::to_string(cpuNo));
    }
    return ptr[index];
}

unsigned int CpuMetrics::size() const{
    return cpuNo;
}

}