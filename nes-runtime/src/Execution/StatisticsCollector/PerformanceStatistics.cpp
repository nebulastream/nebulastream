#include <Execution/StatisticsCollector/PerformanceStatistics.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

void PerformanceStatistics::collect(){
    profiler.stopProfiling();

    profiler.getCount();
}

std::string PerformanceStatistics::getType() const {
    return "PerformanceStatistics";
}

void PerformanceStatistics::startProfiling() {
    profiler.startProfiling();
}

PerformanceStatistics::PerformanceStatistics() {
    profiler = Profiler();
}

} // namespace NES::Runtime::Execution
