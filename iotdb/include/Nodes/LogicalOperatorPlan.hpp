#ifndef LOGICAL_OPERATOR_PLAN_HPP
#define LOGICAL_OPERATOR_PLAN_HPP

#include <API/InputQuery.hpp>
#include <Nodes/Operators//LogicalOperators/LogicalOperatorNode.hpp>


namespace NES {

class LogicalOperatorPlan;
typedef std::shared_ptr<LogicalOperatorPlan> LogicalOperatorPlanPtr ;

class LogicalOperatorPlan {
public:
    LogicalOperatorPlan() = delete;
    LogicalOperatorPlan(const Stream& stream);
    LogicalOperatorPlan(const StreamPtr& stream);
    LogicalOperatorPlan(const InputQueryPtr inputQuery);
    void fromSubOperator(const NodePtr subOp);

    void prettyPrint() const;
    void fromQuery(const InputQueryPtr inputQuery);
    static LogicalOperatorPlan from(Stream& stream);
    LogicalOperatorPlan& filter(const UserAPIExpression& predicate);
    LogicalOperatorPlan& map(const AttributeField& resultField, const Predicate& predicate);
    LogicalOperatorPlan& join(const LogicalOperatorPlan& subQuery,
                              const JoinPredicatePtr joinPred);
    LogicalOperatorPlan& windowByKey(const AttributeFieldPtr& onKey,
                                     const WindowTypePtr& windowType,
                                     const WindowAggregationPtr& aggregation);
    LogicalOperatorPlan& window(const WindowTypePtr& windowType, const WindowAggregationPtr& aggregation);
    LogicalOperatorPlan& print(std::ostream& out=std::cout);
    /**
     * @brief merge another logical plan tree into current plan tree
     * @param platn logical plan tree to merge
     * @param opId the operator to be mount at
     */
    bool mountAsSuccessor(const LogicalOperatorPlanPtr& plan, const std::string& opId);
    bool mountAsPredecessor(const LogicalOperatorPlanPtr& plan, const std::string& opId);
    // LogicalOperatorPlan& to(const std::string& name);
    // LogicalOperatorPlan& join(const )
    NodePtr getOperatorNodeById(const std::string& id) const;
    NodePtr getRoot() const;
private:
    void printHelper(NodePtr op, size_t depth, size_t indent) const;
    void fromQueryHelper(const NodePtr op, const NodePtr parentOp);
    size_t getNextOperatorId() { return ++ operatorId; };
    // NodePtr getOperatorNodeByIdHelper(const NodePtr& op);
private:
    /**
     * @brief the operator id in the same logical plan tree must be unique
     */
    size_t operatorId = -1;
    NodePtr root;
};

}
#endif // LOGICAL_OPERATOR_PLAN_HPP
