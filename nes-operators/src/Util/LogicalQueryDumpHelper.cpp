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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/LogicalQueryDumpHelper.hpp>
#include <fmt/format.h>

namespace NES
{

LogicalQueryDumpHelper::LogicalQueryDumpHelper(std::ostream& out) : out(out), processedDag({}), layerCalcQueue({})
{
}

void LogicalQueryDumpHelper::dump(const QueryPlan& plan)
{
    const std::vector<std::shared_ptr<Operator>> rootOperators = plan.getRootOperators();
    dump(rootOperators);
}

void LogicalQueryDumpHelper::dump(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    /// Don't crash NES just because we failed to print the queryplan.
    try
    {
        const size_t maxWidth = calculateLayers(rootOperators);
        const std::stringstream asciiOutput = drawTree(maxWidth);
        dumpAndUseUnicodeBoxDrawing(asciiOutput.str());
    }
    catch (const std::exception& exc)
    {
        out << "Failed to print queryplan with exception `" << exc.what() << "`\n";
    }
}

size_t LogicalQueryDumpHelper::calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    size_t maxWidth = 0;
    for (auto rootOp : rootOperators)
    {
        layerCalcQueue.emplace_back(std::move(rootOp), std::vector<std::shared_ptr<PrintNode>>());
    }
    std::set<OperatorId> alreadySeen = {};
    size_t currentDepth = 0;
    size_t numberOfNodesOnThisLayer = rootOperators.size();
    size_t numberOfNodesOnNextLayer = 0;
    while (not layerCalcQueue.empty())
    {
        const NodePtr currentNode = layerCalcQueue.front().node;
        const auto parentPtrs = layerCalcQueue.front().parents;
        layerCalcQueue.pop_front();
        numberOfNodesOnThisLayer--;

        const std::vector<NodePtr> children = currentNode->getChildren();
        const std::string text = fmt::format("{:q}", *currentNode);
        const size_t width = text.size();
        const std::shared_ptr<Operator> currentOp = std::static_pointer_cast<Operator>(currentNode);
        const auto id = currentOp->getId();
        auto layerNode = std::make_shared<PrintNode>(PrintNode{text, {}, parentPtrs, {}, false, id});
        if (not alreadySeen.emplace(id).second)
        {
            NES_WARNING("Bug: added the same node multiple times")
        }
        /// Create next layer only if depth has increased
        if (processedDag.size() > currentDepth)
        {
            processedDag.at(currentDepth).nodes.emplace_back(layerNode);
            processedDag.at(currentDepth).width += width;
        }
        else
        {
            processedDag.emplace_back(Layer{{layerNode}, width});
        }
        /// Now that the current Node has been created, we can save its pointer in the parents.
        for (const auto& parent : parentPtrs)
        {
            parent->children.emplace_back(layerNode);
        }
        /// Only add children to the queue if they haven't been added yet (this is the case if one node has multiple parents)
        for (const auto& child : children)
        {
            queueChild(alreadySeen, numberOfNodesOnThisLayer, numberOfNodesOnNextLayer, currentDepth, child, layerNode);
        }
        if (numberOfNodesOnThisLayer == 0)
        {
            /// One character between each node on this layer:
            processedDag.at(currentDepth).width += processedDag.at(currentDepth).nodes.size() - 1;
            maxWidth = std::max(processedDag.at(currentDepth).width, maxWidth);
            numberOfNodesOnThisLayer = numberOfNodesOnNextLayer;
            numberOfNodesOnNextLayer = 0;
            ++currentDepth;
        }
    }
    return maxWidth;
}

void LogicalQueryDumpHelper::queueChild(
    const std::set<OperatorId>& alreadySeen,
    size_t& numberOfNodesOnThisLayer,
    size_t& numberOfNodesOnNextLayer,
    const size_t currentDepth,
    const NodePtr& child,
    const std::shared_ptr<PrintNode>& layerNode)
{
    std::shared_ptr<Operator> childOp = std::static_pointer_cast<Operator>(child);
    const OperatorId childId = childOp->getId();
    auto queueIt = std::ranges::find_if(layerCalcQueue, [&](const QueueItem& queueItem) { return childId == queueItem.node->getId(); });
    const auto seenIt = alreadySeen.find(childId);
    if (queueIt == layerCalcQueue.end() && seenIt == alreadySeen.end())
    {
        /// The child is neither in the queue yet nor already in `processedDag`.
        layerCalcQueue.emplace_back(std::move(childOp), std::vector<std::shared_ptr<PrintNode>>{layerNode});
        ++numberOfNodesOnNextLayer;
    }
    else if (seenIt != alreadySeen.end())
    {
        /// Child is in `processedDag` already. Replace it with dummies up to the current layer.
        bool found = false;
        for (size_t depthLayer = 0; depthLayer <= currentDepth; ++depthLayer)
        {
            for (size_t i = 0; i < processedDag.at(depthLayer).nodes.size(); ++i)
            {
                if (insertDummies(depthLayer, currentDepth, i, childId, childOp, queueIt, numberOfNodesOnNextLayer))
                {
                    found = true;
                    break;
                }
            }
            if (found)
            {
                break;
            }
        }
    }
    else if (queueIt != layerCalcQueue.end())
    {
        /// If the child is in the queue, save that it has another parent:
        const size_t childIndex = queueIt - layerCalcQueue.begin();
        queueIt->parents.push_back(layerNode);
        /// If the child is scheduled to be printed on this layer, that's wrong. Let's move it.
        if (childIndex < numberOfNodesOnThisLayer)
        {
            --numberOfNodesOnThisLayer;
            ++numberOfNodesOnNextLayer;
            if (childIndex < layerCalcQueue.size() - 1)
            {
                const auto temp = *queueIt;
                layerCalcQueue.erase(queueIt);
                /// TODO #685 Test all of these cases: I want to reduce crossings of branches so I try to insert the child at a good place
                /// in the queue. My suspicion is that `childIndex` on the next layer is good.
                if (layerCalcQueue.size() > numberOfNodesOnThisLayer + childIndex)
                {
                    layerCalcQueue.insert(layerCalcQueue.begin() + static_cast<int64_t>(numberOfNodesOnThisLayer + childIndex), temp);
                }
                else if (layerCalcQueue.size() > numberOfNodesOnThisLayer)
                {
                    layerCalcQueue.insert(layerCalcQueue.begin() + static_cast<int64_t>(numberOfNodesOnThisLayer), temp);
                }
                else
                {
                    layerCalcQueue.push_back(temp);
                }
            }
        }
    }
}

bool LogicalQueryDumpHelper::insertDummies(
    const size_t startDepth,
    const size_t endDepth,
    const size_t nodesIndex,
    const OperatorId toBeReplacedId,
    std::shared_ptr<Operator> toBeReplaced,
    const auto& queueIt,
    size_t& numberOfNodesOnNextLayer)
{
    const auto node = processedDag.at(startDepth).nodes.at(nodesIndex);
    if (node->id == toBeReplacedId)
    {
        /// TODO #685 Remove the node's children (if they exist) recursively from processedDag and reinsert them.
        /// TODO #685 What if one of its children has multiple parents?
        auto parents = node->parents;
        for (auto depthDiff = startDepth; depthDiff < endDepth; ++depthDiff)
        {
            auto dummy = std::make_shared<PrintNode>(PrintNode{"|", {}, parents, {}, true, INVALID_OPERATOR_ID});
            if (depthDiff == startDepth)
            {
                processedDag.at(depthDiff).nodes.at(nodesIndex) = dummy;
            }
            else
            {
                /// Here we use `nodesIndex` as a "heuristic" of where to put the dummy
                auto nodes = processedDag.at(depthDiff).nodes;
                if (nodes.size() > nodesIndex)
                {
                    nodes.insert(nodes.begin() + static_cast<int64_t>(nodesIndex), dummy);
                }
                else
                {
                    processedDag.at(depthDiff).nodes.emplace_back(dummy);
                }
            }
            for (const auto& parent : parents)
            {
                if (depthDiff == startDepth)
                {
                    auto it = std::ranges::find(parent->children, node);
                    *it = dummy;
                }
                else
                {
                    parent->children.emplace_back(dummy);
                }
            }
            /// Prepare next iteration
            parents = {dummy};
        }
        /// Add the final dummy as parent of the to be drawn child
        if (queueIt == layerCalcQueue.end())
        {
            layerCalcQueue.emplace_back(std::move(toBeReplaced), std::move(parents));
            numberOfNodesOnNextLayer++;
        }
        else
        {
            queueIt->parents.push_back(parents.at(0));
        }
        return true;
    }
    return false;
}

std::stringstream LogicalQueryDumpHelper::drawTree(const size_t maxWidth) const
{
    std::stringstream asciiOutput;
    /// TODO #685 Adjust perNodeWidth if it * numberOfNodesOnThisLayer is more than width of this layer. Maybe do that in `calculateLayers`.
    for (const auto& currentLayer : processedDag)
    {
        const size_t numberOfNodesInCurrentDepth = currentLayer.nodes.size();
        auto branchLineAbove = std::string(maxWidth, ' ');
        std::stringstream nodeLine;
        const size_t perNodeWidth = maxWidth / numberOfNodesInCurrentDepth;
        for (size_t nodeIdx = 0; const auto& currentNode : currentLayer.nodes)
        {
            const size_t currentMiddleIndex = nodeLine.view().size() + (perNodeWidth / 2);
            for (const auto parentPos : currentNode->parentPositions)
            {
                if (currentMiddleIndex < parentPos)
                {
                    printAsciiBranch(BranchCase::ChildFirst, currentMiddleIndex, branchLineAbove);
                    for (size_t j = currentMiddleIndex + 1; j < parentPos; ++j)
                    {
                        printAsciiBranch(BranchCase::NoConnector, j, branchLineAbove);
                    }
                    printAsciiBranch(BranchCase::ParentLast, parentPos, branchLineAbove);
                }
                else if (currentMiddleIndex > parentPos)
                {
                    printAsciiBranch(BranchCase::ParentFirst, parentPos, branchLineAbove);
                    for (size_t j = parentPos + 1; j < currentMiddleIndex; ++j)
                    {
                        printAsciiBranch(BranchCase::NoConnector, j, branchLineAbove);
                    }
                    printAsciiBranch(BranchCase::ChildLast, currentMiddleIndex, branchLineAbove);
                }
                else
                {
                    printAsciiBranch(BranchCase::Direct, currentMiddleIndex, branchLineAbove);
                }
            }
            nodeLine << fmt::format("{:^{}}", currentNode->text, perNodeWidth);
            for (const auto& child : currentNode->children)
            {
                child->parentPositions.emplace_back(currentMiddleIndex);
            }
            /// Add padding between nodes on the same layer if there's still another one.
            if (nodeIdx + 1 < numberOfNodesInCurrentDepth)
            {
                nodeLine << " ";
            }
            ++nodeIdx;
        }
        asciiOutput << fmt::format("{}\n", branchLineAbove);
        asciiOutput << nodeLine.rdbuf() << "\n";
    };
    return asciiOutput;
}
namespace
{
/// The name means this is the first/middle/last connector [to the child|from the parent] on the line.
constexpr char CHILD_FIRST_BRANCH = '<'; /// '┌'
constexpr char CHILD_MIDDLE_BRANCH = 'T'; /// '┬'
constexpr char CHILD_LAST_BRANCH = '>'; /// '┐'
constexpr char ONLY_CONNECTOR = '|'; /// '│'
constexpr char NO_CONNECTOR_BRANCH = '-'; /// '─'
constexpr char PARENT_FIRST_BRANCH = '['; /// '└'
constexpr char PARENT_MIDDLE_BRANCH = 'V'; /// '┴'
constexpr char PARENT_LAST_BRANCH = ']'; /// '┘'
constexpr char PARENT_CHILD_FIRST_BRANCH = '{'; /// '├'
constexpr char PARENT_CHILD_MIDDLE_BRANCH = '+'; /// '┼'
constexpr char PARENT_CHILD_LAST_BRANCH = '}'; /// '┤'

std::map<char, std::string> getAsciiToUnicode()
{
    return {
        {CHILD_FIRST_BRANCH, "┌"},
        {CHILD_MIDDLE_BRANCH, "┬"},
        {CHILD_LAST_BRANCH, "┐"},
        {ONLY_CONNECTOR, "│"},
        {NO_CONNECTOR_BRANCH, "─"},
        {PARENT_FIRST_BRANCH, "└"},
        {PARENT_MIDDLE_BRANCH, "┴"},
        {PARENT_LAST_BRANCH, "┘"},
        {PARENT_CHILD_FIRST_BRANCH, "├"},
        {PARENT_CHILD_MIDDLE_BRANCH, "┼"},
        {PARENT_CHILD_LAST_BRANCH, "┤"}};
}
}


void LogicalQueryDumpHelper::printAsciiBranch(const BranchCase toPrint, const size_t position, std::string& output)
{
    switch (toPrint)
    {
        case BranchCase::ParentFirst:
            switch (output[position])
            {
                default:
                case ' ':
                    output[position] = PARENT_FIRST_BRANCH;
                    break;
                case NO_CONNECTOR_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case ONLY_CONNECTOR:
                case CHILD_FIRST_BRANCH:
                    output[position] = PARENT_CHILD_FIRST_BRANCH;
                    break;
                case CHILD_MIDDLE_BRANCH:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                case CHILD_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                case PARENT_MIDDLE_BRANCH:
                    /// What we want is already there
                    break;
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
            }
            break;
        case BranchCase::ParentLast:
            switch (output[position])
            {
                default:
                case ' ':
                    output[position] = PARENT_LAST_BRANCH;
                    break;
                case NO_CONNECTOR_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case CHILD_FIRST_BRANCH:
                    output[position] = PARENT_CHILD_FIRST_BRANCH;
                    break;
                case CHILD_MIDDLE_BRANCH:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                case ONLY_CONNECTOR:
                case CHILD_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case PARENT_MIDDLE_BRANCH:
                case PARENT_LAST_BRANCH:
                    /// What we want is already there
                    break;
            }
            break;
        case BranchCase::ChildFirst:
            switch (output[position])
            {
                default:
                case ' ':
                    output[position] = CHILD_FIRST_BRANCH;
                    break;
                case NO_CONNECTOR_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                    output[position] = PARENT_CHILD_FIRST_BRANCH;
                    break;
                case PARENT_MIDDLE_BRANCH:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                case CHILD_LAST_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
            }
            break;
        case BranchCase::ChildLast:
            switch (output[position])
            {
                default:
                case ' ':
                    output[position] = CHILD_LAST_BRANCH;
                    break;
                case NO_CONNECTOR_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                    output[position] = PARENT_CHILD_FIRST_BRANCH;
                    break;
                case PARENT_MIDDLE_BRANCH:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                case CHILD_FIRST_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
            }
            break;
        case BranchCase::Direct:
            switch (output[position])
            {
                case ' ':
                    output[position] = ONLY_CONNECTOR;
                    break;
                case PARENT_FIRST_BRANCH:
                    output[position] = PARENT_CHILD_FIRST_BRANCH;
                    break;
                case PARENT_MIDDLE_BRANCH:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                default:
                    NES_DEBUG("Direct only child: unexpected input. The printed queryplan will probably be incorreclty represented.")
                    break;
            }
            break;
        case BranchCase::NoConnector:
            switch (output[position])
            {
                case ' ':
                    output[position] = NO_CONNECTOR_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case CHILD_FIRST_BRANCH:
                case CHILD_LAST_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
                case ONLY_CONNECTOR:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                default:
                    NES_DEBUG("No connector: unexpected input. The printed queryplan will probably be incorreclty represented.")
                    break;
            }
            break;
        default:
            NES_ERROR("unreachable.")
            break;
    }
}

void LogicalQueryDumpHelper::dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const
{
    size_t line = 0;
    const auto asciiToUnicode = getAsciiToUnicode();
    for (auto character : asciiOutput)
    {
        if (line % 2 == 0)
        {
            auto it = asciiToUnicode.find(character);
            if (it != asciiToUnicode.end())
            {
                out << it->second;
                continue;
            }
        }
        out << character;
        if (character == '\n')
        {
            line++;
        }
    }
}
}
