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
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <gtest/gtest_prod.h>

namespace NES
{

class QueryPlan;

class Node;
using NodePtr = std::shared_ptr<Node>;
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
    std::ostream& out;

    struct PrintNode;
    struct PrintNode
    {
        /// String representation of the Node
        std::string text;
        /// Position of the parents' connectors (where this node wants to connect to). Filled in while drawing.
        std::vector<size_t> parentPositions;
        std::vector<std::shared_ptr<PrintNode>> parents;
        std::vector<std::shared_ptr<PrintNode>> children;
        /// A dummy is actual a vertical branch: '|'
        bool isDummy;
        OperatorId id;
    };
    /// Each entry is a layer in the dag. It holds information on each node of that depth layer and its cumulative width.
    struct Layer
    {
        std::vector<std::shared_ptr<PrintNode>> nodes;
        size_t width;
    };

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

    std::vector<Layer> processedDag;
    struct QueueItem
    {
        std::shared_ptr<Operator> node;
        /// Saves the (already processed) node that queued this node. And if we find more parents, the vector has room for them too.
        std::vector<std::shared_ptr<PrintNode>> parents;
    };
    std::deque<QueueItem> layerCalcQueue;

    /// Converts the `Node`s to `PrintNode`s in the `processedDag` structure which knows about they layer the node should appear on and how
    /// wide the node and the layer is (in terms of ASCII characters).
    size_t calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators);
    /// Decides how to queue the children of the current node depending on whether we already queued it or already put in `processedDag`.
    ///
    /// There are four cases:
    /// - The new node (child to be processed) is not yet in `processedDag` nor in the queue: just queue it.
    /// - The new node is in the queue: Then we note in the queue that it has another parent.
    /// - The new node is in the `alreadySeen` list and therefore in `processedDag`. Then we need to change its layer and insert dummies
    /// between its former position and its new one. Finally queue it.
    /// - The new node is in the `alreadySeen` list and in the queue. Same as above, but note that it has another parent instead of queueing it.
    void queueChild(
        const std::set<OperatorId>& alreadySeen,
        size_t& numberOfNodesOnThisLayer,
        size_t& numberOfNodesOnNextLayer,
        size_t depth,
        const NodePtr& child,
        const std::shared_ptr<PrintNode>& layerNode);
    /// Removes the Node at `nodesIndex` on `startDepth`, replacing it with dummies on each layer until `endDepth` is reached (all in `processedDag`).
    bool insertDummies(
        size_t startDepth,
        size_t endDepth,
        size_t nodesIndex,
        OperatorId toBeReplacedId,
        std::shared_ptr<Operator> toBeReplaced,
        const auto& queueIt,
        size_t& numberOfNodesOnNextLayer);
    [[nodiscard]] std::stringstream drawTree(size_t maxWidth) const;
    /// Prints `toPrint` at position, but depending on what is already there, eg if the parent has a connector to a child on the right and
    /// we want to print another child underneath, replace the PARENT_LAST_BRANCH (┘) connector with a PARENT_CHILD_LAST_BRANCH (┤).
    static void printAsciiBranch(BranchCase toPrint, size_t position, std::string& output);
    void dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const;
};

}
