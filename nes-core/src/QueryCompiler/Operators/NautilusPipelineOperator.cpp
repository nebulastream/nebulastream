#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>

namespace NES {
namespace QueryCompilation {

OperatorNodePtr NautilusPipelineOperator::create(std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline) {
    return std::make_shared<NautilusPipelineOperator>(NautilusPipelineOperator(nautilusPipeline));
}

NautilusPipelineOperator::NautilusPipelineOperator(std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline)
    : OperatorNode(id), UnaryOperatorNode(id), nautilusPipeline(nautilusPipeline) {}

std::string NautilusPipelineOperator::toString() const { return "NautilusPipelineOperator"; }

OperatorNodePtr NautilusPipelineOperator::copy() { return create(nautilusPipeline); }

}// namespace QueryCompilation
}// namespace NES