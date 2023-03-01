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

#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>

namespace NES::Runtime::Execution {

PipelineRuntime::PipelineRuntime(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper,
                                 std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage,
                                 uint64_t pipelineId)
    : changeDetectorWrapper(std::move(changeDetectorWrapper)),
      nautilusExecutablePipelineStage(nautilusExecutablePipelineStage),
      pipelineId(pipelineId) {}

void PipelineRuntime::collect() {
    auto runtime = nautilusExecutablePipelineStage->getRuntimePerBuffer();

    if(runtime != 0){
        std::cout << "PipelineRuntime " << runtime << " Microseconds" << std::endl;

        //todo normalize runtime for change detection
        /*if (changeDetectorWrapper->insertValue(runtime)){
            std::cout << "Change detected" << std::endl;
        }*/
    }
}

std::string PipelineRuntime::getType() const {
    return "PipelineRuntime";
}

} // namespace NES::Runtime::Execution
