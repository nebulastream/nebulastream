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

#ifndef NES_DUPLICATEOPERATORELIMINATIONRULE_HPP
#define NES_DUPLICATEOPERATORELIMINATIONRULE_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

    class Node;
    using NodePtr = std::shared_ptr<Node>;

    class OperatorNode;
    using OperatorNodePtr = std::shared_ptr<OperatorNode>;

    class FilterLogicalOperatorNode;
    using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;

    class ProjectionLogicalOperatorNode;
    using ProjectionLogicalOperatorNodePtr = std::shared_ptr<ProjectionLogicalOperatorNode>;

    class LogicalUnaryOperatorNode;
    using LogicalUnaryOperatorNodePtr = std::shared_ptr<LogicalUnaryOperatorNode>;

    class SourceLogicalOperatorNode;
    using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

}

namespace NES::Optimizer {

    class DuplicateOperatorEliminationRule;
    using DuplicateOperatorEliminationRulePtr = std::shared_ptr<DuplicateOperatorEliminationRule>;

    class DuplicateOperatorEliminationRule : public BaseRewriteRule {
      public:
        /**
         * Eliminates duplicate filter and projection operators from the query plan.
         * in case of an projection every source operator branch is treated separate.
         * @param queryPlan Query plan to rewrite
         * @return rewritten query plan
         */
        QueryPlanPtr apply(QueryPlanPtr queryPlan) override;
        static DuplicateOperatorEliminationRulePtr create();
        virtual ~DuplicateOperatorEliminationRule() = default;

      private:
        explicit DuplicateOperatorEliminationRule();

        /**
         * Function to determine if two filters are duplicates.
         * @param filter1 first filter node
         * @param filter2 second filter node
         * @return if the two filters are duplicates
         */
        bool static areFiltersDuplicates(const FilterLogicalOperatorNodePtr& filter1, const FilterLogicalOperatorNodePtr& filter2);

        /**
         * Function to determine if two projection nodes are duplicates.
         * @param projection1 first projection node
         * @param projection2 second projection node
         * @return if the two projection nodes are duplicates
         */
        bool static areProjectionsDuplicates(const ProjectionLogicalOperatorNodePtr& projection1, const ProjectionLogicalOperatorNodePtr& projection2);

        /**
         * Checks if the given node is a duplicate of any other node in the given container.
         * In case it is, the duplicate node is removed from the query plan.
         *
         * @tparam Container Operators
         * @tparam CompareFunc Function
         * @param operators Operators to eliminate duplicates from
         * @param areDuplicates Function to determine if two operators are duplicates
         */
        template<typename Container, typename CompareFunc>
        static void eliminateDuplicateNodes(Container& operators, CompareFunc areDuplicates);
    };


}
#endif//NES_DUPLICATEOPERATORELIMINATIONRULE_HPP