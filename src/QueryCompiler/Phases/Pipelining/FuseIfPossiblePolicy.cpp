#include <QueryCompiler/Phases/Pipelining/FuseIfPossiblePolicy.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperatorsForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

PipelineBreakerPolicyPtr FuseIfPossiblePolicy::create() {
    return std::make_shared<FuseIfPossiblePolicy>();
}

bool FuseIfPossiblePolicy::isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) {
    return (physicalOperator->instanceOf<PhysicalOperators::PhysicalMapOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalFilterOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalProjectOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalJoinBuildOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>() ||
        physicalOperator->instanceOf<PhysicalOperators::PhysicalSliceMergingOperator>());
}
}}