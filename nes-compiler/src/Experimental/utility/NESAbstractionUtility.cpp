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

#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionForNode.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionIfNode.hpp>
#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionWriteNode.hpp>
#include <Experimental/utility/NESAbstractionUtility.hpp>
/**
  * @brief Create a simple NESAbstractionTree used for testing.
  * @return std::unique_ptr<NESAbstractionTree>
  */
std::shared_ptr<NESAbstractionTree> NESAbstractionUtility::createSimpleNESAbstractionTree(
    uint64_t numTuples, std::vector<uint64_t> indexes, std::vector<NESAbstractionNode::BasicType> types) {
    // Define the ForNode.
    std::shared_ptr<NESAbstractionNode> forNode =
        std::make_shared<NESAbstractionForNode>(0, true, numTuples, indexes, types);

    // Define the IfNode.
    std::vector<int32_t> comparisonValues{42};
    std::vector<NESAbstractionIfNode::ComparisonType> comparisons{
        NESAbstractionIfNode::ComparisonType::SIGNED_INT_LT};
    std::shared_ptr<NESAbstractionNode> ifNode =
        std::make_shared<NESAbstractionIfNode>(1, false, comparisons,
                                                comparisonValues);

    // Define the If-BinOpNode
    std::unique_ptr<NESAbstractionBinOpNode::Operand> ifConstantOperand =
        std::make_unique<NESAbstractionBinOpNode::ConstantOperand>(
            1, NESAbstractionNode::BasicType::INT32);
    std::unique_ptr<NESAbstractionBinOpNode::Operand> elseConstantOperand =
        std::make_unique<NESAbstractionBinOpNode::ConstantOperand>(
            0, NESAbstractionNode::BasicType::INT32);
    std::unique_ptr<NESAbstractionBinOpNode::Operand> ifFieldAccessOperand =
        std::make_unique<NESAbstractionBinOpNode::FieldAccessOperand>(
            0, NESAbstractionNode::BasicType::INT32);
    std::unique_ptr<NESAbstractionBinOpNode::Operand> elseFieldAccessOperand =
        std::make_unique<NESAbstractionBinOpNode::FieldAccessOperand>(
            0, NESAbstractionNode::BasicType::INT32);

    std::shared_ptr<NESAbstractionBinOpNode> ifBinOpNode =
        std::make_shared<NESAbstractionBinOpNode>(
            2, false, std::move(ifConstantOperand),
            std::move(ifFieldAccessOperand),
            NESAbstractionBinOpNode::Operation::add);
    std::shared_ptr<NESAbstractionBinOpNode> elseBinOpNode =
        std::make_shared<NESAbstractionBinOpNode>(
            3, false, std::move(elseConstantOperand),
            std::move(elseFieldAccessOperand),
            NESAbstractionBinOpNode::Operation::add);

    // Define the WriteNode.
    std::vector<NESAbstractionNode::BasicType> outputTypes{
        NESAbstractionNode::BasicType::INT32};
    std::shared_ptr<NESAbstractionNode> writeNode =
        std::make_shared<NESAbstractionWriteNode>(4, false, outputTypes);

    // Establish the hierarchy.
    ifBinOpNode->setChildNode(writeNode);
    elseBinOpNode->setChildNode(std::move(writeNode));

    ifNode->setChildNode(std::move(ifBinOpNode));
    ifNode->setChildNode(std::move(elseBinOpNode));

    forNode->setChildNode(std::move(ifNode));

    // Create NESAbstractionTree. Set ForNode as root node.
    uint64_t queryPlanId = 0;
    uint64_t depth = 3;
    uint64_t numNodes = 3;
    return std::make_shared<NESAbstractionTree>(std::move(forNode), queryPlanId,
                                                depth, numNodes);
}
