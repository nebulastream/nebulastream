#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/BranchMisses.hpp>

namespace NES::Runtime::Execution {

void BranchMisses::collect(){
    profiler.stopProfiling();

    profiler.getCount(eventId);
}

std::string BranchMisses::getType() const {
    return "BranchMisses";
}

void BranchMisses::startProfiling() {
    profiler.startProfiling();
}

BranchMisses::BranchMisses(Profiler profiler) : profiler(profiler) {
    eventId = profiler.getEventId(PERF_COUNT_HW_BRANCH_MISSES);
}

} // namespace NES::Runtime::Execution
