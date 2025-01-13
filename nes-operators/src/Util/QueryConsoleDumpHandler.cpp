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
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <fmt/format.h>
#include "Identifiers/Identifiers.hpp"
#include "Operators/Operator.hpp"

namespace NES
{

QueryConsoleDumpHandler::QueryConsoleDumpHandler(std::ostream& out) : out(out), treeProperties({}), layerCalcQueue({})
{
}

std::shared_ptr<QueryConsoleDumpHandler> QueryConsoleDumpHandler::create(std::ostream& out)
{
    return std::make_shared<QueryConsoleDumpHandler>(out);
}

void QueryConsoleDumpHandler::dump(const QueryPlanPtr& plan)
{
    dump(*plan);
}

void QueryConsoleDumpHandler::dump(const QueryPlan& plan)
{
    const std::vector<std::shared_ptr<Operator>> rootOperators = plan.getRootOperators();
    dump(rootOperators);
}
/// Prints a tree like graph of the queryplan to the stream this class was instatiated with.
///
/// Caveats:
/// - See the [issue](https://github.com/nebulastream/nebulastream-public/issues/685).
/// - The replacing of ASCII branches with Unicode box drawing symbols relies on every even line being branches.
void QueryConsoleDumpHandler::dump(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    const size_t maxWidth = calculateLayers(rootOperators);
    const std::string asciiOutput = drawTree(maxWidth);
    dumpAndUseUnicodeBoxDrawing(asciiOutput);
}

/// Converts the `Node`s to `PrintNode`s in the `treeProperties` structure which knows about they layer the node should appear on and how
/// wide the node and the layer is (in terms of ASCII characters).
size_t QueryConsoleDumpHandler::calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    size_t maxWidth = 0;
    for (auto rootOp : rootOperators)
    {
        layerCalcQueue.emplace_back(std::move(rootOp), std::vector<std::shared_ptr<PrintNode>>());
    }
    std::vector<OperatorId> alreadySeen = {};
    size_t depth = 0;
    size_t numberOfNodesOnThisLayer = rootOperators.size();
    size_t numberOfNodesOnNextLayer = 0;
    while (!layerCalcQueue.empty())
    {
        const NodePtr currentNode = layerCalcQueue.front().first;
        const auto parentPtrs = layerCalcQueue.front().second;
        layerCalcQueue.pop_front();
        numberOfNodesOnThisLayer--;

        const std::vector<NodePtr> children = currentNode->getChildren();
        const std::string text = fmt::format("{:q}", *currentNode);
        const size_t width = text.size();
        const std::shared_ptr<Operator> currentOp = std::static_pointer_cast<Operator>(currentNode);
        const auto id = currentOp->getId();
        auto layerNode = std::make_shared<PrintNode>(PrintNode{text, {}, parentPtrs, {}, false, id});
        alreadySeen.push_back(id);
        /// Create next layer only if depth has increased
        if (treeProperties.size() > depth)
        {
            treeProperties[depth].nodes.emplace_back(layerNode);
            treeProperties[depth].width += width;
        }
        else
        {
            treeProperties.emplace_back(Layer{{layerNode}, width});
        }
        /// Now that the current Node has been created, we can save its pointer in the parents.
        for (const auto& parent : parentPtrs)
        {
            parent->children.emplace_back(layerNode);
        }
        /// Only add children to the queue if they haven't been added yet (this is the case if one node has multiple parents)
        for (const auto& child : children)
        {
            queueChild(alreadySeen, numberOfNodesOnThisLayer, numberOfNodesOnNextLayer, depth, child, layerNode);
        }

        if (!layerCalcQueue.empty())
        {
            if (numberOfNodesOnThisLayer == 0)
            {
                /// One character between each node on this layer:
                treeProperties[depth].width += treeProperties[depth].nodes.size() - 1;
                maxWidth = std::max(treeProperties[depth].width, maxWidth);
                numberOfNodesOnThisLayer = numberOfNodesOnNextLayer;
                numberOfNodesOnNextLayer = 0;
                depth++;
            }
        }
    }
    return maxWidth;
}

/// Decides how to queue the children of the current node depending on whether we already queued it or already put in `treeProperties`.
///
/// There are four cases:
/// - The new node (child to be processed) is not yet in `treeProperties` nor in the queue: just queue it.
/// - The new node is in the queue: Then we note in the queue that it has another parent.
/// - The new node is in the `alreadySeen` list and therefore in `treeProperties`. Then we need to change its layer and insert dummies
/// between its former position and its new one. Finally queue it.
/// - The new node is in the `alreadySeen` list and in the queue. Same as above, but note that it has another parent instead of queueing it.
void QueryConsoleDumpHandler::queueChild(
    std::vector<OperatorId> alreadySeen,
    size_t& numberOfNodesOnThisLayer,
    size_t& numberOfNodesOnNextLayer,
    const size_t& depth,
    const NodePtr& child,
    const std::shared_ptr<PrintNode>& layerNode)
{
    std::shared_ptr<Operator> childOp = std::static_pointer_cast<Operator>(child);
    const OperatorId childId = childOp->getId();
    auto queueIt = std::ranges::find_if(
        layerCalcQueue,
        [&](const std::pair<std::shared_ptr<Operator>, std::vector<std::shared_ptr<PrintNode>>>& queueItem)
        { return childId == queueItem.first->getId(); });
    const auto seenIt = std::ranges::find(alreadySeen, childId);
    if (queueIt == layerCalcQueue.end() && seenIt == alreadySeen.end())
    {
        /// The child is neither in the queue yet nor already in `treeProperties`.
        layerCalcQueue.emplace_back(std::move(childOp), std::vector<std::shared_ptr<PrintNode>>{layerNode});
        ++numberOfNodesOnNextLayer;
    }
    else if (seenIt != alreadySeen.end())
    {
        /// Child is in `treeProperties` already. Replace it with a dummies up to the current layer.
        bool found = false;
        for (size_t depthLayer = 0; depthLayer <= depth; ++depthLayer)
        {
            for (size_t i = 0; i < treeProperties[depthLayer].nodes.size(); ++i)
            {
                if (insertDummies(depthLayer, depth, i, childId, childOp, queueIt, numberOfNodesOnNextLayer))
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
        queueIt->second.push_back(layerNode);
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

/// Removes the Node at `nodesIndex` on `startDepth`, replacing it with dummies on each layer until `endDepth` is reached (all in `treeProperties`).
bool QueryConsoleDumpHandler::insertDummies(
    const size_t startDepth,
    const size_t endDepth,
    const size_t nodesIndex,
    const OperatorId toBeReplacedId,
    std::shared_ptr<Operator> toBeReplaced,
    const auto& queueIt,
    size_t& numberOfNodesOnNextLayer)
{
    const auto node = treeProperties[startDepth].nodes[nodesIndex];
    if (node->id == toBeReplacedId)
    {
        /// TODO #685 Remove the node's children (if they exist) recursively from treeProperties and reinsert them.
        /// TODO #685 What if one of its children has multiple parents?
        auto parents = node->parents;
        for (auto depthDiff = startDepth; depthDiff < endDepth; ++depthDiff)
        {
            auto dummy = std::make_shared<PrintNode>(PrintNode{"|", {}, parents, {}, true, INVALID_OPERATOR_ID});
            if (depthDiff == startDepth)
            {
                treeProperties[depthDiff].nodes[nodesIndex] = dummy;
            }
            else
            {
                /// Here we use `nodesIndex` as a "heuristic" of where to put the dummy
                auto nodes = treeProperties[depthDiff].nodes;
                if (nodes.size() > nodesIndex)
                {
                    nodes.insert(nodes.begin() + static_cast<int64_t>(nodesIndex), dummy);
                }
                else
                {
                    treeProperties[depthDiff].nodes.emplace_back(dummy);
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
            queueIt->second.push_back(parents.at(0));
        }
        return true;
    }
    return false;
}

std::string QueryConsoleDumpHandler::drawTree(const size_t maxWidth) const
{
    std::string asciiOutput;
    size_t depth = 0;
    /// TODO #685 Adjust perNodeWidth if it * numberOfNodesOnThisLayer is more than width of this layer. Maybe do that in `calculateLayers`.
    while (depth < treeProperties.size())
    {
        const size_t nodesAmount = treeProperties[depth].nodes.size();
        auto branchLineAbove = std::string(maxWidth, ' ');
        std::string nodeLine;
        size_t perNodeWidth = maxWidth / nodesAmount;
        for (size_t i = 0; i < nodesAmount; ++i)
        {
            auto [text, parentPositions, parents, children, isDummy, id] = *treeProperties[depth].nodes[i];
            const size_t currentMiddleIndex = nodeLine.size() + (perNodeWidth / 2);
            for (const auto parentPos : parentPositions)
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
            nodeLine += fmt::format("{:^{}}", text, perNodeWidth);
            for (const auto& child : children)
            {
                child->parentPositions.emplace_back(currentMiddleIndex);
            }
            if (i + 1 < nodesAmount)
            {
                nodeLine += ' ';
            }
        }
        asciiOutput += fmt::format("{}\n", branchLineAbove);
        asciiOutput += fmt::format("{}\n", nodeLine);
        ++depth;
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


void QueryConsoleDumpHandler::printAsciiBranch(const BranchCase toPrint, const size_t position, std::string& output)
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

void QueryConsoleDumpHandler::dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const
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
