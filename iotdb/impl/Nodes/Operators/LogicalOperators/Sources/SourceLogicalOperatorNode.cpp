#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <API/InputQuery.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const DataSourcePtr source)
    : LogicalOperatorNode(), source(source) {
}

bool SourceLogicalOperatorNode::equal(const NodePtr rhs) const {
    if(this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperatorNode>();
        // todo check if the sink is the same
        return true;
    }
    return false;
}

const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << NES::toString(source) << ")";
    return ss.str();
}

LogicalOperatorNodePtr createSourceLogicalOperatorNode(const DataSourcePtr source) {
    return std::make_shared<SourceLogicalOperatorNode>(source);
}

DataSourcePtr SourceLogicalOperatorNode::getDataSource() {
    return source;
}

}
