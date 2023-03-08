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

#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <utility>

namespace NES::Runtime::Execution {

PipelineSelectivity::PipelineSelectivity(std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper,
                                         std::shared_ptr<NautilusExecutablePipelineStage> nautilusExecutablePipelineStage)
    : changeDetectorWrapper(std::move(changeDetectorWrapper)),
      nautilusExecutablePipelineStage(std::move(nautilusExecutablePipelineStage)),
      selectivity(0.0){}

void PipelineSelectivity::collect() {
    auto numberOfInputTuples = nautilusExecutablePipelineStage->getNumberOfInputTuples();
    auto numberOfOutputTuples = nautilusExecutablePipelineStage->getNumberOfOutputTuples();

    if(numberOfInputTuples != 0){
        selectivity = (double) numberOfOutputTuples / numberOfInputTuples;
        std::cout << "PipelineSelectivity " << selectivity << std::endl;

        if (changeDetectorWrapper->insertValue(selectivity)){
            std::cout << "Change detected" << std::endl;
        }
    }

}

double PipelineSelectivity::getSelectivity(){
    return selectivity;
}

std::string PipelineSelectivity::getType() const {
    return "PipelineSelectivity";
}

} // namespace NES::Runtime::Execution