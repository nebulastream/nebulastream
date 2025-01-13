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
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include "Operators/Operator.hpp"

namespace NES
{

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Node;
using NodePtr = std::shared_ptr<Node>;
/// Dumps query plans to an output stream
class QueryConsoleDumpHandler
{
public:
    virtual ~QueryConsoleDumpHandler() = default;
    static std::shared_ptr<QueryConsoleDumpHandler> create(std::ostream& out);
    explicit QueryConsoleDumpHandler(std::ostream& out);

    void dump(const QueryPlanPtr& plan);
    void dump(const QueryPlan& plan);
    void dump(const std::vector<std::shared_ptr<Operator>>& rootOperators);

private:
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

    std::vector<Layer> treeProperties;
    std::deque<std::pair<std::shared_ptr<Operator>, std::vector<std::shared_ptr<PrintNode>>>> layerCalcQueue;

    size_t calculateLayers(const std::vector<std::shared_ptr<Operator>>& rootOperators);
    void queueChild(
        std::vector<OperatorId> alreadySeen,
        size_t& numberOfNodesOnThisLayer,
        size_t& numberOfNodesOnNextLayer,
        const size_t& depth,
        const NodePtr& child,
        const std::shared_ptr<PrintNode>& layerNode);
    bool insertDummies(
        size_t startDepth,
        size_t endDepth,
        size_t nodesIndex,
        OperatorId toBeReplacedId,
        std::shared_ptr<Operator> toBeReplaced,
        const auto& queueIt,
        size_t& numberOfNodesOnNextLayer);
    [[nodiscard]] std::string drawTree(size_t maxWidth) const;
    /// Prints `toPrint` at position, but depending on what is already there, eg if the parent has a connector to a child on the right and
    /// we want to print another child underneath, replace the PARENT_LAST_BRANCH (┘) connector with a PARENT_CHILD_LAST_BRANCH (┤).
    static void printAsciiBranch(BranchCase toPrint, size_t position, std::string& output);
    void dumpAndUseUnicodeBoxDrawing(const std::string& asciiOutput) const;
};

}
