//
// Created by pgrulich on 20.03.23.
//

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_
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
        std::shared_ptr<NES::Runtime::Execution::Operators::MockedPipelineExecutionContext> ctx;
    };
    void appendPipeline(std::shared_ptr<PhysicalOperatorPipeline> pipeline,
                        std::shared_ptr<NES::Runtime::Execution::Operators::MockedPipelineExecutionContext> ctx) {
        Pipeline pipe = {std::move(pipeline), std::move(ctx)};
        pipelines.emplace_back(pipe);
    }
    Pipeline& getPipeline(uint64_t index) {
        NES_ASSERT(pipelines.size() > index, "Pipeline with index " << index << " dose not exist!");
        return pipelines[index];
    }

  private:
    std::vector<Pipeline> pipelines;
};

}// namespace NES::Runtime::Execution
#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TPCH_PIPELINEPLAN_HPP_
