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

#ifndef NES_EXECUTION_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_

#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <tuple>
#include <utility>
#include <vector>
namespace NES::Runtime::Execution {

class PipelinePlan {
  public:
    struct Pipeline {
        std::shared_ptr<PhysicalOperatorPipeline> pipeline;
        std::shared_ptr<MockedPipelineExecutionContext> ctx;
    };
    void appendPipeline(std::shared_ptr<PhysicalOperatorPipeline> pipeline, std::shared_ptr<MockedPipelineExecutionContext> ctx) {
        Pipeline pipe = {std::move(pipeline), std::move(ctx)};
        pipelines.emplace_back(pipe);
    }
    Pipeline& getPipeline(uint64_t index) {
        NES_ASSERT(pipelines.size() > index, "Pipeline with index " << index << " does not exist!");
        return pipelines[index];
    }

  private:
    std::vector<Pipeline> pipelines;
};

}// namespace NES::Runtime::Execution
#endif // NES_EXECUTION_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_
