#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

namespace QueryCompilation {

class TranslateToPhysicalOperators;
typedef std::shared_ptr<TranslateToPhysicalOperators> TranslateToPhysicalOperatorsPtr;

class PhysicalQueryPlan;
typedef std::shared_ptr<PhysicalQueryPlan> PhysicalQueryPlanPtr;

}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
