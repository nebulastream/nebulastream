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

#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/StatisticsCollector/BranchMisses.hpp>

namespace NES::Runtime::Execution {

BranchMisses::BranchMisses(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper,
                           const std::shared_ptr<Profiler>& profiler,
                           uint64_t normalizationWindowSize)
    : profiler(profiler),
      normalizer(normalizationWindowSize, std::move(changeDetectorWrapper)),
      branchMisses(0) {
    eventId = profiler->addEvent(PERF_COUNT_HW_BRANCH_MISSES);
}

bool BranchMisses::collect(){
    branchMisses = profiler->getCount(eventId);

    if (branchMisses != 0){
        return normalizer.normalizeValue(branchMisses);
    }
    return false;
}

std::any BranchMisses::getStatisticValue() {
    return branchMisses;
}

} // namespace NES::Runtime::Execution
