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
#include <Execution/StatisticsCollector/CacheMisses.hpp>

namespace NES::Runtime::Execution {

CacheMisses::CacheMisses(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper, std::shared_ptr<Profiler> profiler)
    : changeDetectorWrapper(std::move(changeDetectorWrapper)),
      profiler(profiler) {
    eventId = profiler->getEventId(PERF_COUNT_HW_CACHE_MISSES);
}

void CacheMisses::collect(){
    profiler->stopProfiling();

    auto cacheMisses = profiler->getCount(eventId);
    std::cout << "Cache Misses: " << cacheMisses << std::endl;

    //todo normalize cache misses for change detection
    /*if (changeDetectorWrapper->insertValue(cacheMisses)){
            std::cout << "Change detected" << std::endl;
    }*/
}

std::string CacheMisses::getType() const {
    return "CacheMisses";
}

void CacheMisses::startProfiling() {
    profiler->startProfiling();
}

} // namespace NES::Runtime::Execution