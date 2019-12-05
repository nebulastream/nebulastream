#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <API/InputQuery.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <set>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/ptr_container/serialize_ptr_vector.hpp>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

namespace iotdb {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

enum OperatorType {
  SOURCE_OP,
  FILTER_OP,
  AGGREGATION_OP,
  SORT_OP,
  JOIN_OP,
  SET_OP,
  WINDOW_OP,
  KEYBY_OP,
  MAP_OP,
  SINK_OP
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
};

class Operator {
 public:
  virtual ~Operator();
  virtual const OperatorPtr copy() const = 0;
  size_t cost;
  int operatorId;
  std::vector<OperatorPtr> childs{};
  OperatorPtr parent;
  virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) = 0;
  virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) = 0;
  virtual const std::string toString() const = 0;
  virtual OperatorType getOperatorType() const = 0;
  virtual bool equals(const Operator &_rhs);

  int getOperatorId() { return this->operatorId; };
  void setOperatorId(int operatorId) { this->operatorId = operatorId; };
  bool isScheduled() { return this->scheduled; };
  void markScheduled(bool scheduled) { this->scheduled = scheduled; };

  /**
   * @brief traverses recursively through the operatory tree and returns the operators as a flattened set
   */
  std::set<OperatorType> flattenedTypes(bool traverse_children);

 private:
  bool scheduled = false;

  friend class boost::serialization::access;

  template<class Archive>
  void serialize(Archive &ar, unsigned) {
    ar & BOOST_SERIALIZATION_NVP(cost)
        & BOOST_SERIALIZATION_NVP(operatorId)
        & BOOST_SERIALIZATION_NVP(childs)
        & BOOST_SERIALIZATION_NVP(parent);
  }
};

const OperatorPtr createAggregationOperator(const AggregationSpec &aggr_spec);
const OperatorPtr createFilterOperator(const PredicatePtr &predicate);
const OperatorPtr createJoinOperator(const JoinPredicatePtr &join_spec);
const OperatorPtr createKeyByOperator(const Attributes &keyby_spec);
const OperatorPtr createMapOperator(AttributeFieldPtr attr, PredicatePtr ptr);
const OperatorPtr createSinkOperator(const DataSinkPtr &sink);
const OperatorPtr createSortOperator(const Sort &sort_spec);
const OperatorPtr createSourceOperator(const DataSourcePtr &source);
const OperatorPtr createWindowOperator(const WindowDefinitionPtr &window_definition);

} // namespace iotdb

#endif // OPERATOR_OPERATOR_H
