#include <Monitoring/MetricValues/CPU.hpp>
#include <Util/Logger.hpp>

namespace NES {
CPU::CPU(const unsigned int cpuNo): cpuNo(cpuNo) {
    ptr = nullptr;
    if (cpuNo > 0) {
        ptr = new CpuStats[cpuNo];
    }
}

// Implementation of [] operator.  This function must return a
// reference as array element can be put on left side
CpuStats& CPU::operator[](unsigned int index)
{
    if (index >= cpuNo) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(index) + ">=" + std::to_string(cpuNo));
    }
    return ptr[index];
}

unsigned int CPU::size() const{
    return cpuNo;
}

}