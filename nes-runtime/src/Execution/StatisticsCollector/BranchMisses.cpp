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

BranchMisses::BranchMisses(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper, std::shared_ptr<Profiler> profiler)
    : changeDetectorWrapper(std::move(changeDetectorWrapper)),
      profiler(profiler) {
    eventId = profiler->getEventId(PERF_COUNT_HW_BRANCH_MISSES);
}

void BranchMisses::collect(){
    profiler->stopProfiling();

    auto branchMisses = profiler->getCount(eventId);
    std::cout << "BranchMisses: " << branchMisses << std::endl;

    //todo normalize branch misses for change detection
    /*if (changeDetectorWrapper->insertValue(branchMisses)){
            std::cout << "Change detected" << std::endl;
    }*/
}

std::string BranchMisses::getType() const {
    return "BranchMisses";
}

void BranchMisses::startProfiling() {
    profiler->startProfiling();
}

} // namespace NES::Runtime::Execution
