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
#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

class BasicValue;
using BasicValuePtr = std::shared_ptr<BasicValue>;

class ArrayPhysicalType;
using ArrayPhysicalTypePtr = std::shared_ptr<ArrayPhysicalType>;

namespace Compiler {
class JITCompiler;
using JITCompilerPtr = std::shared_ptr<JITCompiler>;
}// namespace Compiler

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

namespace Execution {
class OperatorHandler;
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;

class ExecutablePipelineStage;
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;

class ExecutablePipeline;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;

}// namespace Execution

}// namespace Runtime

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class JoinLogicalOperatorNode;
using JoinLogicalOperatorNodePtr = std::shared_ptr<JoinLogicalOperatorNode>;

namespace Join {
class LogicalJoinDefinition;
using LogicalJoinDefinitionPtr = std::shared_ptr<LogicalJoinDefinition>;

class JoinOperatorHandler;
using JoinOperatorHandlerPtr = std::shared_ptr<JoinOperatorHandler>;
}// namespace Join

namespace CEP {

class CEPOperatorHandler;
using CEPOperatorHandlerPtr [[maybe_unused]] = std::shared_ptr<CEPOperatorHandler>;
}// namespace CEP

namespace Windowing {

class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;

class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

class AbstractWindowHandler;
using AbstractWindowHandlerPtr = std::shared_ptr<AbstractWindowHandler>;

}// namespace Windowing

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

namespace QueryCompilation {

class PipelineContext;
using PipelineContextPtr = std::shared_ptr<PipelineContext>;

class CodeGenerator;
using CodeGeneratorPtr = std::shared_ptr<CodeGenerator>;

enum JoinBuildSide { Left, Right };

class QueryCompilationError;
using QueryCompilationErrorPtr = std::shared_ptr<QueryCompilationError>;

class QueryCompilationRequest;
using QueryCompilationRequestPtr = std::shared_ptr<QueryCompilationRequest>;

class QueryCompilationResult;
using QueryCompilationResultPtr = std::shared_ptr<QueryCompilationResult>;

class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;

class QueryCompilerOptions;
using QueryCompilerOptionsPtr = std::shared_ptr<QueryCompilerOptions>;

class OperatorPipeline;
using OperatorPipelinePtr = std::shared_ptr<OperatorPipeline>;

class LowerLogicalToPhysicalOperators;
using LowerLogicalToPhysicalOperatorsPtr = std::shared_ptr<LowerLogicalToPhysicalOperators>;

class PhysicalOperatorProvider;
using PhysicalOperatorProviderPtr = std::shared_ptr<PhysicalOperatorProvider>;

class GeneratableOperatorProvider;
using GeneratableOperatorProviderPtr = std::shared_ptr<GeneratableOperatorProvider>;

class LowerPhysicalToGeneratableOperators;
using LowerPhysicalToGeneratableOperatorsPtr = std::shared_ptr<LowerPhysicalToGeneratableOperators>;

class LowerToExecutableQueryPlanPhase;
using LowerToExecutableQueryPlanPhasePtr = std::shared_ptr<LowerToExecutableQueryPlanPhase>;

class PipelineQueryPlan;
using PipelineQueryPlanPtr = std::shared_ptr<PipelineQueryPlan>;

class AddScanAndEmitPhase;
using AddScanAndEmitPhasePtr = std::shared_ptr<AddScanAndEmitPhase>;

class BufferOptimizationPhase;
using BufferOptimizationPhasePtr = std::shared_ptr<BufferOptimizationPhase>;

class PredicationOptimizationPhase;
using PredicationOptimizationPhasePtr = std::shared_ptr<PredicationOptimizationPhase>;

class CodeGenerationPhase;
using CodeGenerationPhasePtr = std::shared_ptr<CodeGenerationPhase>;

class PipeliningPhase;
using PipeliningPhasePtr = std::shared_ptr<PipeliningPhase>;

class OperatorFusionPolicy;
using OperatorFusionPolicyPtr = std::shared_ptr<OperatorFusionPolicy>;

class DataSinkProvider;
using DataSinkProviderPtr = std::shared_ptr<DataSinkProvider>;

class DataSourceProvider;
using DataSourceProviderPtr = std::shared_ptr<DataSourceProvider>;
namespace Phases {

class PhaseFactory;
using PhaseFactoryPtr = std::shared_ptr<PhaseFactory>;

}// namespace Phases

namespace GeneratableOperators {
class GeneratableOperator;
using GeneratableOperatorPtr = std::shared_ptr<GeneratableOperator>;

class GeneratableWindowAggregation;
using GeneratableWindowAggregationPtr = std::shared_ptr<GeneratableWindowAggregation>;

}// namespace GeneratableOperators

namespace PhysicalOperators {
class PhysicalOperator;
using PhysicalOperatorPtr = std::shared_ptr<PhysicalOperator>;

}// namespace PhysicalOperators

}// namespace QueryCompilation

}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
