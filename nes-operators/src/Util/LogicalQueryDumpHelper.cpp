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
#include <iterator>
#include <map>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/LogicalQueryDumpHelper.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

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
        NES_ERROR("Failed to print queryplan with exception: {}\n", exc.what());
    }
}

size_t LogicalQueryDumpHelper::calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators)
{
    size_t maxWidth = 0;
    size_t currentDepth = 0;
    std::unordered_set<OperatorId> alreadySeen = {};
    NodesPerLayerCounter nodesPerLayer = {.current = rootOperators.size(), .next = 0};
    for (auto rootOp : rootOperators)
    {
        layerCalcQueue.emplace_back(std::move(rootOp), std::vector<std::weak_ptr<PrintNode>>());
    }
    while (not layerCalcQueue.empty())
    {
        const auto currentNode = NES::Util::as_if<Operator>(layerCalcQueue.front().node);
        const auto parentPtrs = layerCalcQueue.front().parents;
        layerCalcQueue.pop_front();
        nodesPerLayer.current--;

        const std::string currentNodeAsString = fmt::format("{:q}", *currentNode);
        const size_t width = currentNodeAsString.size();
        const auto id = currentNode->getId();
        auto layerNode = std::make_shared<PrintNode>(
            currentNodeAsString, std::vector<size_t>{}, parentPtrs, std::vector<std::shared_ptr<PrintNode>>{}, false, id);
        if (not alreadySeen.emplace(id).second)
        {
            NES_ERROR("Bug: added the same node multiple times.")
        }
        /// Create next layer only if depth has increased.
        if (processedDag.size() > currentDepth)
        {
            processedDag.at(currentDepth).nodes.emplace_back(layerNode);
            processedDag.at(currentDepth).layerWidth += width;
        }
        else
        {
            processedDag.emplace_back(Layer{.nodes = {layerNode}, .layerWidth = width});
        }
        /// Now that the current Node has been created, we can save its pointer in the parents.
        for (const auto& parent : parentPtrs)
        {
            parent.lock()->children.emplace_back(layerNode);
        }
        /// Only add children to the queue if they haven't been added yet (this is the case if one node has multiple parents)
        for (const auto& child : currentNode->getChildren())
        {
            const auto childOp = NES::Util::as_if<Operator>(child);
            queueChild(alreadySeen, nodesPerLayer, currentDepth, childOp, layerNode);
        }
        if (nodesPerLayer.current == 0)
        {
            /// One character between each node on this layer:
            processedDag.at(currentDepth).layerWidth += processedDag.at(currentDepth).nodes.size() - 1;
            maxWidth = std::max(processedDag.at(currentDepth).layerWidth, maxWidth);
            nodesPerLayer.current = nodesPerLayer.next;
            nodesPerLayer.next = 0;
            ++currentDepth;
        }
    }
    return maxWidth;
}

void LogicalQueryDumpHelper::queueChild(
    const std::unordered_set<OperatorId>& alreadySeen,
    NodesPerLayerCounter& nodesPerLayer,
    const size_t currentDepth,
    const std::shared_ptr<Operator>& child,
    const std::shared_ptr<PrintNode>& layerNode)
{
    const OperatorId childId = child->getId();
    auto queueIt
        = std::ranges::find_if(layerCalcQueue, [childId](const QueueItem& queueItem) { return childId == queueItem.node->getId(); });
    const auto seenIt = alreadySeen.find(childId);
    if (queueIt == layerCalcQueue.end() && seenIt == alreadySeen.end())
    {
        /// Just queue the child normally.
        layerCalcQueue.emplace_back(child, std::vector<std::weak_ptr<PrintNode>>{layerNode});
        ++nodesPerLayer.next;
    }
    else if (seenIt != alreadySeen.end())
    {
        /// Child is in `processedDag` already. Replace it with dummies up to the current layer.
        for (size_t depthLayer = 0; depthLayer <= currentDepth; ++depthLayer)
        {
            auto nodes = processedDag.at(depthLayer).nodes;
            auto it = std::ranges::find_if(nodes, [&](const auto& node) { return node->id == childId; });
            if (it != nodes.end())
            {
                const size_t nodeIndex = std::distance(nodes.begin(), it);
                insertVerticalBranches(depthLayer, currentDepth, nodeIndex, child, queueIt, nodesPerLayer);
            }
            else
            {
                /// `it` should never be `end()`, because we only add nodes to `alreadySeen` when we add them to `processedDag`.
                NES_ERROR("Bug: child that was marked as already seen was not found in processedDag.");
            }
        }
    }
    else if (queueIt != layerCalcQueue.end())
    {
        /// If the child is in the queue, save that it has another parent:
        const size_t childIndex = queueIt - layerCalcQueue.begin();
        queueIt->parents.push_back(layerNode);
        /// If the child is scheduled to be printed on this layer we need to correct that, because we are currently queuing _children_ of
        /// this layer.
        if (childIndex < nodesPerLayer.current)
        {
            --nodesPerLayer.current;
            ++nodesPerLayer.next;
            /// Only change the child's position if it is not the last one in the queue. If it is the last one, we can just leave it there.
            if (std::next(queueIt) != layerCalcQueue.end())
            {
                const auto temp = *queueIt;
                layerCalcQueue.erase(queueIt);
                /// TODO #685 Test all of these cases: To reduce crossings of branches, try to insert the child at a good place in the
                /// queue. If the next layer has a similar number of nodes, `childIndex` could be a good place to insert it.
                if (layerCalcQueue.size() > nodesPerLayer.current + childIndex)
                {
                    layerCalcQueue.insert(layerCalcQueue.begin() + static_cast<int64_t>(nodesPerLayer.current + childIndex), temp);
                }
                else if (layerCalcQueue.size() > nodesPerLayer.current)
                {
                    layerCalcQueue.insert(layerCalcQueue.begin() + static_cast<int64_t>(nodesPerLayer.current), temp);
                }
                else
                {
                    layerCalcQueue.push_back(temp);
                }
            }
        }
    }
}

void LogicalQueryDumpHelper::insertVerticalBranches(
    const size_t startDepth,
    const size_t endDepth,
    const size_t nodesIndex,
    std::shared_ptr<Operator> operatorToBeReplaced,
    const std::ranges::borrowed_iterator_t<std::deque<QueueItem>&>& queueIt,
    NodesPerLayerCounter& nodesPerLayer)
{
    const auto node = processedDag.at(startDepth).nodes.at(nodesIndex);

    {
        /// TODO #685 Remove the node's children (if they exist) recursively from processedDag and reinsert them.
        /// TODO #685 What if one of its children has multiple parents?
        auto parents = node->parents;
        for (auto depthDiff = startDepth; depthDiff < endDepth; ++depthDiff)
        {
            auto verticalBranchNode = std::make_shared<PrintNode>(PrintNode{
                .nodeAsString = "|",
                .parentPositions = {},
                .parents = parents,
                .children = {},
                .verticalBranch = true,
                .id = INVALID_OPERATOR_ID});
            if (depthDiff == startDepth)
            {
                processedDag.at(depthDiff).nodes.at(nodesIndex) = verticalBranchNode;
            }
            else
            {
                /// Here we use `nodesIndex` as a "heuristic" of where to put the verticalBranchNode
                auto nodes = processedDag.at(depthDiff).nodes;
                if (nodes.size() > nodesIndex)
                {
                    nodes.insert(nodes.begin() + static_cast<int64_t>(nodesIndex), verticalBranchNode);
                }
                else
                {
                    processedDag.at(depthDiff).nodes.emplace_back(verticalBranchNode);
                }
            }
            for (const auto& parent : parents)
            {
                if (depthDiff == startDepth)
                {
                    auto it = std::ranges::find(parent.lock()->children, node);
                    *it = verticalBranchNode;
                }
                else
                {
                    parent.lock()->children.emplace_back(verticalBranchNode);
                }
            }
            /// Prepare next iteration
            parents = {verticalBranchNode};
        }
        /// Add the final dummy as parent of the to be drawn child
        if (queueIt == layerCalcQueue.end())
        {
            layerCalcQueue.emplace_back(std::move(operatorToBeReplaced), std::move(parents));
            nodesPerLayer.next++;
        }
        else
        {
            queueIt->parents.push_back(parents.at(0));
        }
    }
}

std::stringstream LogicalQueryDumpHelper::drawTree(const size_t maxWidth) const
{
    std::stringstream asciiOutput;
    /// TODO #685 Adjust perNodeWidth if it * numberOfNodesOnThisLayer is more than maxWidth. Maybe do that in `calculateLayers`.
    for (const auto& currentLayer : processedDag)
    {
        const size_t numberOfNodesInCurrentDepth = currentLayer.nodes.size();
        auto branchLineAbove = std::string(maxWidth, ' ');
        std::stringstream nodeLine;
        const size_t availableCenteringSpace = maxWidth - currentLayer.layerWidth;
        const size_t centeringSpacePerNode = availableCenteringSpace / numberOfNodesInCurrentDepth;
        for (size_t nodeIdx = 0; const auto& currentNode : currentLayer.nodes)
        {
            const size_t currentNodeAvailableWidth = currentNode->nodeAsString.size() + centeringSpacePerNode;
            const size_t currentMiddleIndex = nodeLine.view().size() + (currentNodeAvailableWidth / 2);
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
            nodeLine << fmt::format("{:^{}}", currentNode->nodeAsString, currentNodeAvailableWidth);
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
    INVARIANT(position < output.size(), "Position is out of bounds.");
    switch (toPrint)
    {
        case BranchCase::ParentFirst:
            switch (output[position])
            {
                case ' ':
                    output[position] = PARENT_FIRST_BRANCH;
                    break;
                case NO_CONNECTOR_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case ONLY_CONNECTOR:
                    [[fallthrough]];
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
                    [[fallthrough]];
                case PARENT_MIDDLE_BRANCH:
                    /// What we want is already there
                    break;
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                default:
                    break;
            }
            break;
        case BranchCase::ParentLast:
            switch (output[position])
            {
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
                    [[fallthrough]];
                case CHILD_LAST_BRANCH:
                    output[position] = PARENT_CHILD_LAST_BRANCH;
                    break;
                case PARENT_FIRST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case PARENT_MIDDLE_BRANCH:
                    [[fallthrough]];
                case PARENT_LAST_BRANCH:
                    /// What we want is already there
                    break;
                default:
                    break;
            }
            break;
        case BranchCase::ChildFirst:
            switch (output[position])
            {
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
                default:
                    break;
            }
            break;
        case BranchCase::ChildLast:
            switch (output[position])
            {
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
                default:
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
                    NES_DEBUG("Direct only child: unexpected input. The printed queryplan will probably be incorrectly represented.")
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
                    [[fallthrough]];
                case PARENT_LAST_BRANCH:
                    output[position] = PARENT_MIDDLE_BRANCH;
                    break;
                case CHILD_FIRST_BRANCH:
                    [[fallthrough]];
                case CHILD_LAST_BRANCH:
                    output[position] = CHILD_MIDDLE_BRANCH;
                    break;
                case ONLY_CONNECTOR:
                    output[position] = PARENT_CHILD_MIDDLE_BRANCH;
                    break;
                default:
                    NES_DEBUG("No connector: unexpected input. The printed query plan will probably be incorrectly represented.")
                    break;
            }
            break;
        default:
            NES_ERROR("BranchCase is unreachable.")
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
