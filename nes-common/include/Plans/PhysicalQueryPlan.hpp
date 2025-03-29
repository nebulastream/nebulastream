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

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Sinks/Sink.hpp>
#include <Sources/Source.hpp>
#include <Emit.hpp>
#include <PhysicalOperator.hpp>
#include <Scan.hpp>

namespace NES
{

using namespace NES;

// TODO should this consist of unique pointers? In the best case we can completely get rid of it
using OperatorVariant = std::variant<
    std::shared_ptr<SinkLogicalOperator>,
    std::shared_ptr<SourceDescriptorLogicalOperator>,
    std::shared_ptr<PhysicalOperator>,
    std::shared_ptr<Scan>,
    std::shared_ptr<Emit>>;

struct PhysicalOperatorNode
{
    PhysicalOperatorNode(OperatorVariant op) : op(std::move(op)) {};

    std::vector<std::shared_ptr<PhysicalOperatorNode>> getAllLeafNodes()
    {
        NES_DEBUG("Node: Get all leaf nodes for this node");
        std::vector<std::shared_ptr<PhysicalOperatorNode>> leafNodes;

        if (children.empty())
        {
            NES_DEBUG("Node: found no children. Returning itself as leaf.");
            leafNodes.push_back(std::make_shared<PhysicalOperatorNode>(*this));
        }

        for (auto& child : children)
        {
            if (child->children.empty())
            {
                NES_DEBUG("Node: Inserting leaf node to the collection");
                leafNodes.push_back(child);
            }
            else
            {
                NES_DEBUG("Node: Iterating over all children to find more leaf nodes");
                for (const auto& childOfChild : child->children)
                {
                    std::vector<std::shared_ptr<PhysicalOperatorNode>> childrenLeafNodes = childOfChild->getAllLeafNodes();
                    NES_DEBUG("Node: inserting leaf nodes into the collection of leaf nodes");
                    leafNodes.insert(leafNodes.end(), childrenLeafNodes.begin(), childrenLeafNodes.end());
                }
            }
        }
        NES_DEBUG("Node: Found {} leaf nodes", leafNodes.size());
        return leafNodes;
    }

    OperatorVariant op;
    std::vector<std::shared_ptr<PhysicalOperatorNode>> children;
};

struct PhysicalQueryPlan;
std::string printPhysicalQueryPlan(const PhysicalQueryPlan& queryPlan);

struct PhysicalQueryPlan
{
    explicit PhysicalQueryPlan(QueryId queryId) : queryId(queryId) { }
    explicit PhysicalQueryPlan(QueryId queryId, const std::vector<std::shared_ptr<PhysicalOperatorNode>>& rootOperators)
        : queryId(queryId), rootOperators(rootOperators)
    {
    }

    void prependOperatorAsNewRoot(const std::shared_ptr<PhysicalOperatorNode>& operatorNode)
    {
        for (const auto& rootOperator : rootOperators)
        {
            operatorNode->children.push_back(rootOperator);
        }
        rootOperators.clear();
        rootOperators.push_back(operatorNode);
    }

    bool replaceRootOperator(const std::shared_ptr<PhysicalOperatorNode>& oldRoot, const std::shared_ptr<PhysicalOperatorNode>& newRoot)
    {
        for (auto& rootOperator : rootOperators)
        {
            /// compares the pointers and checks if we found the correct operator.
            if (rootOperator == oldRoot)
            {
                rootOperator = newRoot;
                return true;
            }
        }
        return false;
    }

    std::string toString() { return printPhysicalQueryPlan(*this); }

    QueryId queryId;
    std::vector<std::shared_ptr<PhysicalOperatorNode>> rootOperators;
};

inline std::string printOperatorVariantType(const OperatorVariant& op)
{
    std::stringstream ss;
    std::visit(
        [&ss](auto&& arg)
        {
            using T = std::decay_t<decltype(arg)>;
            if (arg == nullptr)
            {
                ss << "Null Operator";
                return;
            }
            if constexpr (std::is_same_v<T, std::shared_ptr<SinkLogicalOperator>>)
            {
                ss << "SinkLogicalOperator";
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<SourceDescriptorLogicalOperator>>)
            {
                ss << "SourceDescriptorLogicalOperator";
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<PhysicalOperator>>)
            {
                ss << "PhysicalOperator";
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Scan>>)
            {
                ss << "Scan";
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Emit>>)
            {
                ss << "Emit";
            }
            else
            {
                ss << "Unknown Type";
            }
        },
        op);
    return ss.str();
}

inline std::string printQueryPlan(const std::vector<std::shared_ptr<PhysicalOperatorNode>>& nodes, int depth = 0)
{
    std::stringstream ss;
    for (const auto& node : nodes)
    {
        if (!node)
        {
            ss << std::string(depth * 2, ' ') << "- Node: Null\n";
            continue;
        }

        // Indent for visualization of the hierarchy
        ss << std::string(depth * 2, ' ') << "- Node: ";

        // Print the variant type
        ss << printOperatorVariantType(node->op);
        ss << '\n';

        // Recur for children
        ss << printQueryPlan(node->children, depth + 1);
    }
    return ss.str();
}

inline std::string printPhysicalQueryPlan(const PhysicalQueryPlan& queryPlan)
{
    std::stringstream ss;
    ss << "Query Plan for Query ID: " << queryPlan.queryId << '\n';
    ss << printQueryPlan(queryPlan.rootOperators);
    return ss.str();
}

}