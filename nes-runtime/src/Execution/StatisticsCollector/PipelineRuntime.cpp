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
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <future>
#include <utility>

namespace NES::Runtime::Execution {

PipelineRuntime::PipelineRuntime(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper,
                                 std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage,
                                 uint64_t normalizationWindowSize)
    : nautilusExecutablePipelineStage(std::move(nautilusExecutablePipelineStage)),
      normalizer(normalizationWindowSize, std::move(changeDetectorWrapper)),
      runtime(0){}

void PipelineRuntime::collect() {
    runtime = nautilusExecutablePipelineStage->getRuntimePerBuffer();

    if (runtime != 0){
        normalizer.normalizeValue(runtime);
    }
}
std::any PipelineRuntime::getStatisticValue() {
    return runtime;
}

} // namespace NES::Runtime::Execution
