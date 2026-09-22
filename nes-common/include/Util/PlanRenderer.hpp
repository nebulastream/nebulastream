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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <map>
#include <memory>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Defines the verbosity level for explaining query plans and operators
enum class ExplainVerbosity : uint8_t
{
    /// Detailed explanation including all available information
    Debug,
    /// Brief explanation with essential information only
    Short
};

/// Branch types for ASCII art rendering
enum class BranchCase : uint8_t
{
    /// └
    ParentFirst,
    /// ┘
    ParentLast,
    /// ┌
    ChildFirst,
    /// ┐
    ChildLast,
    /// │
    Direct,
    /// ─
    NoConnector
};

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

template <typename T>
struct GetRootOperator
{
    auto operator()(const T& op) const { return op.getRootOperators(); }
};

template <typename T>
struct Explain
{
    auto operator()(const T& op, const ExplainVerbosity verbosity) const { return op.explain(verbosity); }
};

template <typename T>
struct GetId
{
    auto operator()(const T& op) const { return op.getId().getRawValue(); }
};

template <typename T>
struct GetChildren
{
    auto operator()(const T& op) const { return op.getChildren(); }
};

/// Maximum display width for a single operator node label before truncation.
constexpr size_t MAX_NODE_DISPLAY_WIDTH = 60;

/// Truncates a string to at most `maxWidth` characters, appending "..." if truncated.
inline std::string truncateNodeLabel(const std::string& label, const size_t maxWidth)
{
    if (label.size() <= maxWidth)
    {
        return label;
    }
    /// Reserve 3 characters for the ellipsis.
    return label.substr(0, maxWidth - 3) + "...";
}

/// Dumps query plans to an output stream
template <typename Plan, typename Operator>
class PlanRenderer
{
public:
    using IdType = std::invoke_result_t<GetId<Operator>, Operator>;
    virtual ~PlanRenderer() = default;
    explicit PlanRenderer(std::ostream& out, ExplainVerbosity verbosity)
        : out(out), verbosity(verbosity), processedDag({}), layerCalcQueue({}) { };

    void dump(const Plan& plan)
    {
        auto rootOperators = GetRootOperator<Plan>{}(plan);
        dump(rootOperators);
    }

    /// Prints a tree like graph of the queryplan to the stream this class was instantiated with.
    ///
    /// Caveats:
    /// - The replacing of ASCII branches with Unicode box drawing symbols relies on every even line being branches.
    void dump(const std::vector<Operator>& rootOperators)
    {
        /// Don't crash NES just because we failed to print the queryplan.
        CPPTRACE_TRY
        {
            size_t maxWidth = calculateLayers(rootOperators);
            /// `insertVerticalBranches` can widen layers that were already finalised (and thus already accounted for in the `maxWidth`
            /// returned above). Recompute so no layer can exceed `maxWidth`, which would underflow the centering math in `drawTree`.
            for (const auto& layer : processedDag)
            {
                maxWidth = std::max(maxWidth, layer.layerWidth);
            }
            const std::stringstream asciiOutput = drawTree(maxWidth);
            dumpAndUseUnicodeBoxDrawing(asciiOutput.str());
        }
        CPPTRACE_CATCH(...)
        {
            NES_ERROR("Failed to print queryplan with following exception:");
            tryLogCurrentException();
        }
    }

private:
    std::ostream& out;
    ExplainVerbosity verbosity;

    struct PrintNode
    {
        std::string nodeAsString;
        /// Position of the parents' connectors (where this node wants to connect to). Filled in while drawing.
        std::vector<size_t> parentPositions;
        std::vector<std::weak_ptr<PrintNode>> parents;
        std::vector<std::shared_ptr<PrintNode>> children;
        /// If true, this node as actually a "dummy node" (not representing an actual node from the queryplan) and just represents a
        /// vertical branch: '|'.
        bool verticalBranch{};
        IdType id{};
    };

    /// Holds information on each node of that layer in the dag and its cumulative width.
    struct Layer
    {
        std::vector<std::shared_ptr<PrintNode>> nodes;
        size_t layerWidth;
    };

    std::vector<Layer> processedDag;

    struct QueueItem
    {
        Operator node;
        /// Saves the (already processed) node that queued this node. And if we find more parents, the vector has room for them too.
        std::vector<std::weak_ptr<PrintNode>> parents;
    };

    std::deque<QueueItem> layerCalcQueue;

    /// Converts the `Node`s to `PrintNode`s in the `processedDag` structure which knows about they layer the node should appear on and how
    /// wide the node and the layer is (in terms of ASCII characters).
    size_t calculateLayers(const std::vector<Operator>& rootOperators)
    {
        size_t maxWidth = 0;
        size_t currentDepth = 0;
        std::unordered_set<IdType> alreadySeen = {};
        NodesPerLayerCounter nodesPerLayer = {rootOperators.size(), 0};
        for (auto rootOp : rootOperators)
        {
            layerCalcQueue.emplace_back(std::move(rootOp), std::vector<std::weak_ptr<PrintNode>>());
        }
        while (not layerCalcQueue.empty())
        {
            const auto currentNode = layerCalcQueue.front().node;
            const auto parentPtrs = layerCalcQueue.front().parents;
            layerCalcQueue.pop_front();
            nodesPerLayer.current--;

            const std::string currentNodeAsString = truncateNodeLabel(Explain<Operator>{}(currentNode, verbosity), MAX_NODE_DISPLAY_WIDTH);
            const size_t width = currentNodeAsString.size();
            const auto id = GetId<Operator>{}(currentNode);
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
                processedDag.emplace_back(Layer{{layerNode}, width});
            }
            /// Now that the current Node has been created, we can save its pointer in the parents.
            for (const auto& parent : parentPtrs)
            {
                /// Re-parenting in `insertVerticalBranches` can drop the last strong reference to a parent `PrintNode` while a
                /// still-queued child holds a `weak_ptr` to it. Dereferencing an expired `weak_ptr` is UB, which asserts-disabled
                /// builds compile out and `CPPTRACE_CATCH` cannot catch, so guard it the same way as the sibling site there.
                const auto parentNode = parent.lock();
                INVARIANT(parentNode != nullptr, "queued node's parent must still be alive");
                if (parentNode == nullptr)
                {
                    NES_WARNING("PlanRenderer: skipping edge from a parent that is no longer alive.");
                    continue;
                }
                parentNode->children.emplace_back(layerNode);
            }
            /// Only add children to the queue if they haven't been added yet (this is the case if one node has multiple parents)
            for (const auto& child : GetChildren<Operator>{}(currentNode))
            {
                queueChild(alreadySeen, nodesPerLayer, currentDepth, child, layerNode);
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

    struct NodesPerLayerCounter
    {
        size_t current;
        size_t next;
    };

    /// Decides how to queue the children of the current node depending on whether we already queued it or already put in `processedDag`.
    ///
    /// There are four cases:
    /// - The new node (child to be processed) is not yet in `processedDag` nor in the queue: just queue it.
    /// - The new node is in the queue: Then we note in the queue that it has another parent.
    /// - The new node is in the `alreadySeen` list and therefore in `processedDag`. Then we need to change its layer and insert vertical
    ///   branches between its former position and its new one. Finally queue it.
    /// - The new node is in the `alreadySeen` list and in the queue. Same as above, but note that it has another parent instead of queueing it.
    void queueChild(
        std::unordered_set<IdType>& alreadySeen,
        NodesPerLayerCounter& nodesPerLayer,
        size_t depth,
        const Operator& child,
        const std::shared_ptr<PrintNode>& layerNode)
    {
        const IdType childId = GetId<Operator>{}(child);
        auto queueIt = std::ranges::find_if(
            layerCalcQueue, [childId](const QueueItem& queueItem) { return childId == GetId<Operator>{}(queueItem.node); });
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
            bool found = false;
            for (size_t depthLayer = 0; depthLayer <= depth; ++depthLayer)
            {
                const auto& nodes = processedDag.at(depthLayer).nodes;
                auto it = std::ranges::find_if(nodes, [&](const auto& node) { return node->id == childId; });
                if (it != nodes.end())
                {
                    const size_t nodeIndex = std::distance(nodes.begin(), it);
                    insertVerticalBranches(depthLayer, depth, nodeIndex, child, queueIt, nodesPerLayer, alreadySeen, layerNode);
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                NES_ERROR("Bug: child that was marked as already seen was not found in processedDag.");
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
                    auto temp = std::move(*queueIt);
                    layerCalcQueue.erase(queueIt);
                    /// To reduce crossings of branches, try to insert the child at a good place in the queue. If the next layer has a
                    /// similar number of nodes, `childIndex` could be a good place to insert it.
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

    /// Removes the Node at `nodesIndex` on `startDepth`, replacing it with nodes that represent vertical branches on each layer until
    /// `endDepth` is reached (all in `processedDag`). The node is re-queued so it will be placed at the correct (deeper) layer.
    void insertVerticalBranches(
        size_t startDepth,
        size_t endDepth,
        size_t nodesIndex,
        Operator operatorToBeReplaced,
        const std::ranges::borrowed_iterator_t<std::deque<QueueItem>&>& queueIt,
        NodesPerLayerCounter& nodesPerLayer,
        std::unordered_set<IdType>& alreadySeen,
        const std::shared_ptr<PrintNode>& reParentingNode)
    {
        const auto node = processedDag.at(startDepth).nodes.at(nodesIndex);

        {
            auto parents = node->parents;
            /// The re-queued node will be placed one layer below its deepest parent (`endDepth + 1`), so the branch column has to
            /// span every layer from the node`s old position down to and including `endDepth`. Using `<=` (not `<`) also guarantees
            /// at least one iteration when `startDepth == endDepth` (a shared node re-parented from its own layer): without it the
            /// original node was never replaced by a placeholder yet still re-queued, so it rendered twice.
            for (auto depthDiff = startDepth; depthDiff <= endDepth; ++depthDiff)
            {
                auto verticalBranchNode = std::make_shared<PrintNode>(PrintNode{"|", {}, parents, {}, true, IdType{}});
                auto& layer = processedDag.at(depthDiff);
                if (depthDiff == startDepth)
                {
                    /// Replace the node in place with a placeholder; the layer keeps the same node count but loses the node`s width.
                    layer.nodes.at(nodesIndex) = verticalBranchNode;
                    layer.layerWidth = layer.layerWidth - node->nodeAsString.size() + 1;
                }
                else
                {
                    /// Here we use `nodesIndex` as a "heuristic" of where to put the verticalBranchNode
                    auto& nodes = layer.nodes;
                    if (nodes.size() > nodesIndex)
                    {
                        nodes.insert(nodes.begin() + static_cast<int64_t>(nodesIndex), verticalBranchNode);
                    }
                    else
                    {
                        nodes.emplace_back(verticalBranchNode);
                    }
                    /// Account for the added placeholder (width 1). Already-finalised layers (`depthDiff < endDepth`) also need its
                    /// inter-node separator (+1); the `endDepth` layer is still being built, so its separators are added wholesale at
                    /// finalisation in `calculateLayers` and must not be counted a second time here.
                    layer.layerWidth += (depthDiff == endDepth) ? 1 : 2;
                }
                for (const auto& parent : parents)
                {
                    /// Assert the invariant (bug in debug builds) but also guard defensively: dereferencing an expired `weak_ptr`
                    /// or writing through `end()` is UB, which asserts-disabled builds compile out and `CPPTRACE_CATCH` cannot catch.
                    const auto parentNode = parent.lock();
                    INVARIANT(parentNode != nullptr, "re-parented node's parent must still be alive");
                    if (parentNode == nullptr)
                    {
                        NES_WARNING("PlanRenderer: skipping re-parenting of a node whose parent is no longer alive.");
                        continue;
                    }
                    if (depthDiff == startDepth)
                    {
                        auto it = std::ranges::find(parentNode->children, node);
                        INVARIANT(it != parentNode->children.end(), "parent must reference the node being re-parented");
                        if (it == parentNode->children.end())
                        {
                            NES_WARNING("PlanRenderer: parent does not reference the node being re-parented; edge left un-rewired.");
                            continue;
                        }
                        *it = verticalBranchNode;
                    }
                    else
                    {
                        parentNode->children.emplace_back(verticalBranchNode);
                    }
                }
                /// Prepare next iteration
                parents = {verticalBranchNode};
            }
            /// The node moves below `reParentingNode`, so record that edge too; otherwise the connection to the re-parenting node
            /// is dropped.
            parents.push_back(reParentingNode);
            /// Remove from alreadySeen so calculateLayers can re-process the node at the correct depth.
            alreadySeen.erase(GetId<Operator>{}(operatorToBeReplaced));
            /// Add the final dummy as parent of the to be drawn child
            if (queueIt == layerCalcQueue.end())
            {
                layerCalcQueue.emplace_back(std::move(operatorToBeReplaced), std::move(parents));
                nodesPerLayer.next++;
            }
            else
            {
                queueIt->parents.push_back(parents.at(0));
                queueIt->parents.push_back(reParentingNode);
            }
        }
    }

    [[nodiscard]] std::stringstream drawTree(size_t maxWidth) const
    {
        std::stringstream asciiOutput;
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

    void dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const
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

    /// Prints `toPrint` at position, but depending on what is already there, e.g. if the parent has a connector to a child on the right and
    /// we want to print another child underneath, replace the PARENT_LAST_BRANCH (┘) connector with a PARENT_CHILD_LAST_BRANCH (┤).
    void printAsciiBranch(const BranchCase toPrint, const size_t position, std::string& output) const
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
                        [[fallthrough]];
                    default:
                        /// What we want is already there
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
                        break;
                }
                break;
            default:
                NES_ERROR("BranchCase is unreachable.")
                break;
        }
    }

    static std::map<char, std::string> getAsciiToUnicode()
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
};

}
