#include <queue>

#include <API/UserAPIExpression.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <OperatorNodes/LogicalOperatorPlan.hpp>
#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>
#include <OperatorNodes/Impl/SourceLogicalOperatorNode.hpp>

#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

BaseOperatorNodePtr createLogicalOperatorNodeFromOperator(const OperatorPtr op) {
    BaseOperatorNodePtr logicalOp = nullptr;
    switch (op->getOperatorType()) {
    case SOURCE_OP:
        logicalOp = createSourceLogicalOperatorNode(nullptr);
        break;
    case FILTER_OP:
        logicalOp = createFilterLogicalOperatorNode(nullptr);
        break;
    case SINK_OP:
        logicalOp = createSinkLogicalOperatorNode(nullptr);
        break;
    case MAP_OP:
        logicalOp = createMapLogicalOperatorNode(nullptr, nullptr);
    default: {
        std::cout << "Unknown operator node" << std::endl;
        break;
    }
    }
    return logicalOp;
}

LogicalOperatorPlan::LogicalOperatorPlan(const InputQueryPtr inputQuery) {
    std::cout << "LogicalOperatorPlan inputQuery" << std::endl;
    root = nullptr;
    this->fromSubOperator(inputQuery->getRoot());
    // this->fromQuery(inputQuery);
}

LogicalOperatorPlan::LogicalOperatorPlan(const StreamPtr& stream) {
}

LogicalOperatorPlan::LogicalOperatorPlan(const Stream& stream)
    : LogicalOperatorPlan(std::make_shared<Stream>(stream)) {

}


void LogicalOperatorPlan::fromSubOperator(const OperatorPtr op) {
    BaseOperatorNodePtr logicalOp = createLogicalOperatorNodeFromOperator(op);

    if (root == nullptr) {
        root = logicalOp;
    } else {
        root->addSuccessor(logicalOp);
        logicalOp->addPredecessor(root);
    }

    BaseOperatorNodePtr tmpRoot = root;
    root = logicalOp;

    auto children = op->getChildren();
    for (auto&& child : children) {
        this->fromSubOperator(child);
    }
    root = tmpRoot;
    // std::cout << root->toString() << std::endl;
}

void LogicalOperatorPlan::fromQuery(const InputQueryPtr inputQuery) {
    fromQueryHelper(inputQuery->getRoot(), nullptr);
}

void LogicalOperatorPlan::fromQueryHelper(const OperatorPtr op, const OperatorPtr parentOp) {
    if (! op) {
        return;
    }
    BaseOperatorNodePtr _parentOp = nullptr;
    BaseOperatorNodePtr _op = createLogicalOperatorNodeFromOperator(op);
    if (parentOp == nullptr) {
        std::cout << "only once" << std::endl;
        root = _op;
    } else {
        _parentOp = createLogicalOperatorNodeFromOperator(parentOp);
        _parentOp->addSuccessor(_op);
        _op->addPredecessor(_parentOp);
    }
    std::cout << _op->toString() << std::endl;
    auto children = op->getChildren();
    for (auto&& child : children) {
        // ERROR: need to create a new pointer, leading to bugs
        fromQueryHelper(child, op);
    }
}

void LogicalOperatorPlan::printHelper(BaseOperatorNodePtr op, size_t depth, size_t indent) const {
    if (! op) {
        return;
    }
    std::cout << std::string(indent*depth, ' ') << op->toString()
              // << ", indent depth: " << indent*depth
              << "[" << op->getOperatorId() << "]"
              << std::endl;
    ++ depth;
    auto children = op->getSuccessors();
    for (auto&& child: children) {
        printHelper(child, depth, indent);
    }
}

void LogicalOperatorPlan::prettyPrint() const {
    this->printHelper(root, 0, 2);
}

LogicalOperatorPlan LogicalOperatorPlan::from(Stream& stream) {
    LogicalOperatorPlan q(stream);
    BaseOperatorNodePtr op = createSourceLogicalOperatorNode(
        createTestDataSourceWithSchema(stream.getSchema()));
    // size_t operatorId = q.getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();
    op->setOperatorId(operatorId);
    q.root = op;
    return q;
}

LogicalOperatorPlan& LogicalOperatorPlan::filter(const UserAPIExpression& predicate) {
    PredicatePtr pred = createPredicate(predicate);
    const BaseOperatorNodePtr op = createFilterLogicalOperatorNode(pred);
    // int operatorId = this->getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();
    op->setOperatorId(operatorId);

    root->addPredecessor(op);
    op->addSuccessor(root);
    root = op;
    return *this;
}

LogicalOperatorPlan& LogicalOperatorPlan::map(const AttributeField& field, const Predicate& predicate) {
    AttributeFieldPtr attr = field.copy();
    PredicatePtr pred = createPredicate(predicate);
    const BaseOperatorNodePtr op = createMapLogicalOperatorNode(attr, pred);
    // size_t operatorId = this->getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();

    op->setOperatorId(operatorId);

    root->addPredecessor(op);
    op->addSuccessor(root);
    root = op;
    return *this;
}

LogicalOperatorPlan& LogicalOperatorPlan::join(const LogicalOperatorPlan& subQuery,
                                               const JoinPredicatePtr joinPred) {
    BaseOperatorNodePtr op = createJoinLogicalOperatorNode(joinPred);
    std::string operatorId = UtilityFunctions::generateUuid();
    op->setOperatorId(operatorId);
    root->addPredecessor(op);
    subQuery.root->addPredecessor(op);
    op->addSuccessor(root);
    op->addSuccessor(subQuery.root);
    root = op;
    return *this;
}

LogicalOperatorPlan& LogicalOperatorPlan::windowByKey(const AttributeFieldPtr& onKey,
                                                      const WindowTypePtr& windowType,
                                                      const WindowAggregationPtr& aggregation) {
    auto windowDefPtr = std::make_shared<WindowDefinition>(onKey, aggregation,
                                                             windowType);
    const BaseOperatorNodePtr op = createWindowLogicalOperatorNode(windowDefPtr);
    // size_t operatorId = this->getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();


    op->setOperatorId(operatorId);

    root->addPredecessor(op);
    op->addSuccessor(root);
    root = op;
    return *this;
}

LogicalOperatorPlan& LogicalOperatorPlan::window(const WindowTypePtr& windowType,
                                                 const WindowAggregationPtr& aggregation) {
    auto windowDefPtr = std::make_shared<WindowDefinition>(aggregation,
                                                           windowType);
    const BaseOperatorNodePtr op = createWindowLogicalOperatorNode(windowDefPtr);
    // size_t operatorId = this->getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();


    op->setOperatorId(operatorId);

    root->addPredecessor(op);
    op->addSuccessor(root);
    root = op;
    return *this;
}

LogicalOperatorPlan& LogicalOperatorPlan::print(std::ostream& out) {
    const BaseOperatorNodePtr op = createSinkLogicalOperatorNode(
        createPrintSinkWithoutSchema(out));
    // size_t operatorId = this->getNextOperatorId();
    std::string operatorId = UtilityFunctions::generateUuid();


    op->setOperatorId(operatorId);

    root->addPredecessor(op);
    op->addSuccessor(root);
    root = op;
    return *this;
}

BaseOperatorNodePtr LogicalOperatorPlan::getOperatorNodeById(const std::string& id) const {
    std::queue<BaseOperatorNodePtr> opList;
    opList.push(root);

    while (! opList.empty()) {
        auto front = opList.front();
        opList.pop();

        if (front->getOperatorId() == id) {
            return front;
        } else {
            auto children = front->getSuccessors();
            for (auto&& child : children) {
                opList.push(child);
            }
        }
    }
    std::cout << "Not found operator " << id << std::endl;
    return nullptr;
}
BaseOperatorNodePtr LogicalOperatorPlan::getRoot() const {
    return root;
}

bool LogicalOperatorPlan::mountAsSuccessor(const LogicalOperatorPlanPtr& plan, const std::string& opId) {
    NES_NOT_IMPLEMENTED
    return false;
}

bool LogicalOperatorPlan::mountAsPredecessor(const LogicalOperatorPlanPtr& plan, const std::string& opId) {
    auto _op = this->getOperatorNodeById(opId);
    if (_op) {
        plan->root->addSuccessor(_op);
        _op->addPredecessor(plan->root);
        return true;
    }
    return false;
}

}
