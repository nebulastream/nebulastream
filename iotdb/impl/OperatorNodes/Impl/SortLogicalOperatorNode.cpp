

#include <iostream>
#include <sstream>
#include <vector>

#include <OperatorNodes/Impl/SortLogicalOperatorNode.hpp>

namespace NES {

SortLogicalOperatorNode::SortLogicalOperatorNode(const Sort& sortSpec) : Operator(), sort_spec_(NES::copy(sortSpec)) {}

// SortLogicalOperatorNode::SortLogicalOperatorNode(const SortLogicalOperatorNode& other) : sort_spec_(NES::copy(other.sort_spec_)) {}

// SortLogicalOperatorNode& SortLogicalOperatorNode::operator=(const SortLogicalOperatorNode& other)
// {
//     if (this != &other) {
//         sort_spec_ = NES::copy(other.sort_spec_);
//     }
//     return *this;
// }

// void SortLogicalOperatorNode::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
// void SortLogicalOperatorNode::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

// const OperatorPtr SortLogicalOperatorNode::copy() const { return std::make_shared<SortLogicalOperatorNode>(*this); }

const std::string SortLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SORT(" << NES::toString(sort_spec_) << ")";
    return ss.str();
}

OperatorType SortLogicalOperatorNode::getOperatorType() const {
    return OperatorType::SORT_OP;
}

// SortLogicalOperatorNode::~SortLogicalOperatorNode() {}

const BaseOperatorNodePtr createSortLogicalOperatorNode(const Sort& sortSpec) {
    return std::make_shared<SortLogicalOperatorNode>(sortSpec);
}

} // namespace NES
