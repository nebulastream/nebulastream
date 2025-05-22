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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <gtest/gtest_prod.h>

namespace NES
{

class QueryPlan;

class Node;

/// Dumps query plans to an output stream
class LogicalQueryDumpHelper
{
public:
    virtual ~LogicalQueryDumpHelper() = default;
    explicit LogicalQueryDumpHelper(std::ostream& out);

    void dump(const QueryPlan& plan);
    /// Prints a tree like graph of the queryplan to the stream this class was instatiated with.
    ///
    /// Caveats:
    /// - See the [issue](https://github.com/nebulastream/nebulastream-public/issues/685) (/// TODO #685).
    /// - The replacing of ASCII branches with Unicode box drawing symbols relies on every even line being branches.
    void dump(const std::vector<std::shared_ptr<Operator>>& rootOperators);

private:
    FRIEND_TEST(LogicalQueryDumpHelperTest, printQuerySourceFilterMapSink);
    FRIEND_TEST(LogicalQueryDumpHelperTest, printQueryMapFilterTwoSinks);
    FRIEND_TEST(LogicalQueryDumpHelperTest, printQuerySourceTwoSinksExtraLong);
    std::ostream& out;

    struct PrintNode;

    struct PrintNode
    {
        std::string nodeAsString;
        /// Position of the parents' connectors (where this node wants to connect to). Filled in while drawing.
        std::vector<size_t> parentPositions;
        std::vector<std::weak_ptr<PrintNode>> parents;
        std::vector<std::shared_ptr<PrintNode>> children;
        /// If true, this node as actually a "dummy node" (not representing an actual node from the queryplan) and just represents a
        /// vertical branch: '|'.
        bool verticalBranch;
        OperatorId id;
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
        std::shared_ptr<Operator> node;
        /// Saves the (already processed) node that queued this node. And if we find more parents, the vector has room for them too.
        std::vector<std::weak_ptr<PrintNode>> parents;
    };

    std::deque<QueueItem> layerCalcQueue;

    /// Converts the `Node`s to `PrintNode`s in the `processedDag` structure which knows about they layer the node should appear on and how
    /// wide the node and the layer is (in terms of ASCII characters).
    size_t calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators);

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
        const std::unordered_set<OperatorId>& alreadySeen,
        NodesPerLayerCounter& nodesPerLayer,
        size_t depth,
        const std::shared_ptr<Operator>& child,
        const std::shared_ptr<PrintNode>& layerNode);
    /// Removes the Node at `nodesIndex` on `startDepth`, replacing it with nodes that represent vertical branches on each layer until
    /// `endDepth` is reached (all in `processedDag`).
    void insertVerticalBranches(
        size_t startDepth,
        size_t endDepth,
        size_t nodesIndex,
        std::shared_ptr<Operator> operatorToBeReplaced,
        const std::ranges::borrowed_iterator_t<std::deque<QueueItem>&>& queueIt,
        NodesPerLayerCounter& nodesPerLayer);
    [[nodiscard]] std::stringstream drawTree(size_t maxWidth) const;
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
    /// Prints `toPrint` at position, but depending on what is already there, e.g. if the parent has a connector to a child on the right and
    /// we want to print another child underneath, replace the PARENT_LAST_BRANCH (┘) connector with a PARENT_CHILD_LAST_BRANCH (┤).
    static void printAsciiBranch(BranchCase toPrint, size_t position, std::string& output);
    void dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const;
};

}
