#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <API/InputQuery.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace NES {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class WindowType;
typedef std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregation;
typedef std::shared_ptr<WindowAggregation> WindowAggregationPtr;
class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

enum OperatorType {
    SOURCE_OP,
    FILTER_OP,
    AGGREGATION_OP,
    SORT_OP,
    JOIN_OP,
    SET_OP,
    WINDOW_OP,
    KEYBY_OP,
    Merge_OP,
    MAP_OP,
    SINK_OP,
    SAMPLE_OP
};

static std::map<OperatorType, std::string> operatorTypeToString{
    {SOURCE_OP, "SOURCE"},
    {FILTER_OP, "FILTER"},
    {AGGREGATION_OP, "AGGREGATION"},
    {SORT_OP, "SORT"},
    {JOIN_OP, "JOIN"},
    {SET_OP, "SET"},
    {WINDOW_OP, "WINDOW"},
    {KEYBY_OP, "KEYBY"},
    {MAP_OP, "MAP"},
    {SINK_OP, "SINK"},
    {SAMPLE_OP, "SAMPLE"},
};

/**
 * @brief Keeping information if an operator is blocking or not.
 * This information is used during the placement operation.
 */
static std::map<OperatorType, bool> operatorIsBlocking{
    {SOURCE_OP, false},
    {FILTER_OP, false},
    {AGGREGATION_OP, true},
    {SORT_OP, true},
    {JOIN_OP, true},
    {SET_OP, true},
    {WINDOW_OP, true},
    {KEYBY_OP, false},
    {MAP_OP, false},
    {SINK_OP, false},
};

class Operator {
  public:
    virtual ~Operator();
    virtual const OperatorPtr copy() const = 0;
    size_t cost;
    virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
    virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
    virtual const std::string toString() const = 0;
    virtual OperatorType getOperatorType() const = 0;
    virtual bool equals(const Operator& _rhs);

    size_t getOperatorId() const { return this->operatorId; };
    void setOperatorId(size_t operatorId) { this->operatorId = operatorId; };
    const std::vector<OperatorPtr> getChildren() const;
    void setChildren(const std::vector<OperatorPtr> children);
    void addChild(const OperatorPtr child);
    const OperatorPtr getParent() const;
    void setParent(const OperatorPtr parent);
    /**
     * @brief traverses recursively through the operatory tree and returns the operators as a flattened set
     */
    std::set<OperatorType> flattenedTypes();

  private:
    size_t operatorId;
    std::vector<OperatorPtr> children;
    OperatorPtr parent;
    std::set<OperatorType> traverseOpTree(bool traverse_children);
};

const OperatorPtr createAggregationOperator(const AggregationSpec& aggr_spec);
const OperatorPtr createFilterOperator(const PredicatePtr predicate);
const OperatorPtr createJoinOperator(const JoinPredicatePtr join_spec);
const OperatorPtr createKeyByOperator(const Attributes& keyby_spec);
const OperatorPtr createMapOperator(AttributeFieldPtr attr, PredicatePtr ptr);
const OperatorPtr createSinkOperator(const DataSinkPtr sink);
const OperatorPtr createSortOperator(const Sort& sort_spec);
const OperatorPtr createSourceOperator(const DataSourcePtr source);
const OperatorPtr createWindowOperator(const WindowDefinitionPtr window_definition);
const OperatorPtr createWindowScanOperator(const SchemaPtr schema);
const OperatorPtr createSampleOperator(const std::string& udfs);
const OperatorPtr createMergeOperator(const SchemaPtr schemaPtr);
}// namespace NES

#endif// OPERATOR_OPERATOR_H
