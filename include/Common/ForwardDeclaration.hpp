#ifndef NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_
#define NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_
#include <memory>
// TODO ALL: use this file instead of declaring types manually in every single file!
// TODO ALL: this is only for runtime components
namespace NES {

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class QueryManager;
typedef std::shared_ptr<QueryManager> QueryManagerPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class GeneratedQueryExecutionPlan;
typedef std::shared_ptr<GeneratedQueryExecutionPlan> GeneratedQueryExecutionPlanPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class QueryCompiler;
typedef std::shared_ptr<QueryCompiler> QueryCompilerPtr;

class SinkMedium;
typedef std::shared_ptr<SinkMedium> DataSinkPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

}// namespace NES

#endif//NES_INCLUDE_COMMON_FORWARDDECLARATION_HPP_
