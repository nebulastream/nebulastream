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

CacheMisses::CacheMisses(std::unique_ptr<ChangeDetector> changeDetector,
                         const std::shared_ptr<Profiler>& profiler,
                         uint64_t normalizationWindowSize)
    : profiler(profiler),
      normalizer(normalizationWindowSize, std::move(changeDetector)) {
      eventId = profiler->addEvent(PERF_COUNT_HW_CACHE_MISSES);
}

bool CacheMisses::collect(){
    cacheMisses = profiler->getCount(eventId);

    return normalizer.normalizeValue(cacheMisses);
}

std::any CacheMisses::getStatisticValue() {
    return cacheMisses;
}

} // namespace NES::Runtime::Execution