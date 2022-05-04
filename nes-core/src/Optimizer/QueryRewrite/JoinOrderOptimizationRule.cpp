/*
Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <set>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include "Optimizer/QueryRewrite/JoinOrderOptimizationRule.hpp"
#include "Windowing/LogicalJoinDefinition.hpp"
#include "Util/AbstractJoinPlanOperator.hpp"

namespace NES::Optimizer {

    JoinOrderOptimizationRulePtr JoinOrderOptimizationRule::create() { return std::make_shared<JoinOrderOptimizationRule>(JoinOrderOptimizationRule()); }

    JoinOrderOptimizationRule::JoinOrderOptimizationRule() = default;

    QueryPlanPtr JoinOrderOptimizationRule::apply(QueryPlanPtr queryPlan) {
        NES_DEBUG("JoinOrderOptimizationRule: Applying JoinOrderOptimizationRule to query " << queryPlan->toString());

        // Check if the QueryPlan has more than 1 join -- if so reordering might be beneficial
        int count = 0;
        std::vector<JoinLogicalOperatorNodePtr> joinOperators;
        for (const auto& rootOperator : queryPlan->getRootOperators()) {
            ++count;
            auto joins = rootOperator->getNodesByType<JoinLogicalOperatorNode>();
            joinOperators.insert(joinOperators.end(), joins.begin(), joins.end());
        }
        if (joinOperators.size() >= 2) {
            NES_DEBUG("Found a total of " << joinOperators.size() << " JoinOperators. Trying to evaluate best join order.")

            // SourceOperators are the logical data streams that are similar to relations in DBMS
            const auto sourceOperators = queryPlan->getSourceOperators();

            std::vector<OptimizerPlanOperatorPtr> sources;
            for (const auto &source : sourceOperators){
                sources.push_back(std::make_shared<OptimizerPlanOperator>(source));
            }

            std::vector<AbstractJoinPlanOperatorPtr> abstractJoinOperators;
            for(auto source: sources){
                abstractJoinOperators.push_back(std::make_shared<AbstractJoinPlanOperator>(source));
            }

            //  Derive a join graph by getting all the join edges between the sources (logical streams)
            std::vector<Join::JoinEdgePtr> joinEdges = retrieveJoinEdges(joinOperators, abstractJoinOperators);
            getHardCodedJoinSelectivities(joinEdges);

            // print joinEdges
            NES_DEBUG(listJoinEdges(joinEdges));

            // Construct dynamic programming table and retrieve the best optimization order
            AbstractJoinPlanOperatorPtr finalPlan = optimizeJoinOrder(sources, joinEdges);

            // extract join order from final plan
            std::any joinOrder = extractJoinOrder(finalPlan);

            // print join order
            std::cout << printJoinOrder(joinOrder) << std::endl;

            queryPlan = updateJoinOrder(queryPlan, finalPlan, sourceOperators);

            NES_DEBUG(queryPlan->toString());

            return queryPlan;

        } else {
            NES_DEBUG("Not enough JoinOperator found to do JoinReordering. Amount of joinOperations: " << joinOperators.size());
            return queryPlan;
        }
    }

    // we need to make sure, that, when we deriveSourceName, we do not query the sourceOperators again, but rather query our list of OptimizerPlanOperators for that exact object
    // with that log. streamName. This would then allows us to use to declare it only once and don't screw up lvl 2.
    std::vector<Join::JoinEdgePtr> JoinOrderOptimizationRule::retrieveJoinEdges(std::vector<JoinLogicalOperatorNodePtr> joinOperators, std::vector<AbstractJoinPlanOperatorPtr> abstractJoinOperators){

        // Iterate over joinOperators and detect all JoinEdges
        std::vector<Join::JoinEdgePtr> joinEdges;
        for (auto join : joinOperators){

            // Construct joinEdge
            std::string leftSource = deriveSourceName(join->getJoinDefinition()->getLeftJoinKey());
            std::string rightSource = deriveSourceName(join->getJoinDefinition()->getRightJoinKey());


            AbstractJoinPlanOperatorPtr leftOperatorNode;
            AbstractJoinPlanOperatorPtr rightOperatorNode;

            for (auto abstractJoinOperator : abstractJoinOperators){

                if(leftSource == abstractJoinOperator->getSourceNode()->getSourceDescriptor()->getLogicalSourceName()){
                    leftOperatorNode = abstractJoinOperator;
                }
                if (rightSource == abstractJoinOperator->getSourceNode()->getSourceDescriptor()->getLogicalSourceName()){
                    rightOperatorNode = abstractJoinOperator;
                }
            }

            NES_DEBUG("Found the following corresponding SourceLogicalOperatorNodes: " << leftOperatorNode->getSourceNode()->toString() << " and " << rightOperatorNode->getSourceNode()->toString());

            Join::JoinEdge joinEdge = Join::JoinEdge(leftOperatorNode, rightOperatorNode, join->getJoinDefinition(), 0.5);
            joinEdges.push_back(std::make_shared<Join::JoinEdge>(joinEdge));
        }

        return joinEdges;

    }

    // optimizing the join order given the join graph
    AbstractJoinPlanOperatorPtr JoinOrderOptimizationRule::optimizeJoinOrder(std::vector<OptimizerPlanOperatorPtr> sources, std::vector<Join::JoinEdgePtr> joins){
        //For each key (number of joined relations) the HashMap includes a HashMap where for each possible sub-combination of joins
        // The optimal one is stored (exists only if it is possible using JoinPredicates)
        std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> subs;

        //Estimate the cardinality for all relations and add them in the HashMap
        // adding empty map for int 1
        subs[1] = std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>();
        NES_DEBUG("Setting the base logical streams for Optimization table: ")

        // CHECK IF I REALLY NEED TO GO OVER abstractJoinOperators here and match them with the source -- could do a simple for loop int i = 0; i < sources.size(); i++ and do it simultaneously for bot vectors.
        for (auto source : sources){
            // insert rows into first level of hashmap
            std::set<OptimizerPlanOperatorPtr> key;
            key.insert(source);
            auto joinSource = std::make_shared<AbstractJoinPlanOperator>(source);
            joinSource->setInvolvedOptimizerPlanOperators(key);
            subs[1].insert(std::pair<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>(key, joinSource));
        }
        NES_DEBUG("For level 1 we are dealing with " << subs[1].size() << " base logical stream(s), while we should have: " << sources.size())

        //Calculate for each level=number of relations involved the optimal combinations
        //and add them to the main HashMap
        // ab level 2 fängt man ja erst an mit diesen Kombinationen.
        // createJoinCandidates gibt wieder eine subs hashmap zurück, bekommt momentane kombis, level und joinEdges
        for (size_t i = 2; i <= sources.size(); i++) {
            NES_DEBUG("Starting with level " << i << " Adding join candidates to dynamic programming hashmap")
            subs = createJoinCandidates(subs, i, joins);
        }

        //Select the plan in which all relations are involved
        //Already optimal plan
        std::set<OptimizerPlanOperatorPtr> sourceSet(sources.begin(), sources.end());
        AbstractJoinPlanOperatorPtr finalPlan = subs[sources.size()][sourceSet];
        return finalPlan;

    }

    std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> JoinOrderOptimizationRule::createJoinCandidates(std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>> subs, int level, std::vector<Join::JoinEdgePtr> joins){
        //Add HashMap for current level (number of involved relations)
        // für das nächste Level eine Schicht
        subs[level] = std::map<std::set<OptimizerPlanOperatorPtr>, AbstractJoinPlanOperatorPtr>();

        //Create relation pairs and add them into the 2-level HashMap
        //(initial step therefore special case)
        if (level == 2) {
            for (auto graphEdge : joins) {
                // as these are two regular streams that are being joint, getting leftSource instead of leftOperator is fine.
                AbstractJoinPlanOperatorPtr plan = std::make_shared<AbstractJoinPlanOperator>(graphEdge->getLeftOperator(), graphEdge->getRightOperator(), graphEdge->getJoinDefinition(), graphEdge->getSelectivity());
                setCosts(plan);
                subs[2][plan->getInvolvedOptimizerPlanOperators()] = plan;
            }
            return subs;
        }

        //Get possible combinations (for a join of 3 relations => only 1 Relation joins 2 already Joined relations possible)
        std::vector<std::vector<int>> combs = getCountCombs(level - 1);

        //Create for each possible combination of joins an AbstractJoinPlanOperator
        //Should a plan with an identical relation combination already exists the cost-efficient
        //plan is added to the level-HashMap
        for (auto comb : combs) {
            // left side of comb is comb[0] and right side is comb[1] as we are dealing with pairs.
            int outerLoopCount = 1;
            for(auto left : subs[comb[0]]){
                std::cout << "Outerloop " << outerLoopCount << " - Setting left id to: " << std::to_string(left.second->getId()) << std::endl;
                int innerLoopCount = 1;
                for (auto right : subs[comb[1]]){
                    std::cout << "     Innerloop " << innerLoopCount << "Setting right ids to: " << std::to_string(right.second->getLeftChild()->getId()) << " x " << std::to_string(right.second->getRightChild()->getId()) << std::endl;

                    // create a new AbstractJoinPlanOperator for each combination therefore and set its costs.
                     std::optional<AbstractJoinPlanOperatorPtr> newPlan = join(left.second, right.second, joins);
                    if (newPlan){
                        setCosts(newPlan.value());
                        // check if newPlan is currently the best
                        // note: if an empty OptimizerPlanOperator is made from belows statement, the operatorCosts of that plan are set to -1
                        AbstractJoinPlanOperatorPtr oldPlan = subs[level][newPlan.value()->getInvolvedOptimizerPlanOperators()]; // this returns for the very same logicalStreams the current best plan

                        if (oldPlan == nullptr  || oldPlan->getCumulativeCosts() > newPlan.value()->getCumulativeCosts()){
                            subs[level][newPlan.value()->getInvolvedOptimizerPlanOperators()] = newPlan.value();
                        }
                    }
                    innerLoopCount++;
                }
                outerLoopCount++;
            }
        }
        return subs;
    }


    std::string JoinOrderOptimizationRule::deriveSourceName(FieldAccessExpressionNodePtr joinKey) {
        std::string joinKeyNode = joinKey->toString();
        size_t pos = joinKeyNode.find('$');
        joinKeyNode = joinKeyNode.substr(0, pos);
        pos = joinKeyNode.find("(") + 1;
        joinKeyNode = joinKeyNode.substr(pos);
        return joinKeyNode;
    }
    std::string JoinOrderOptimizationRule::listJoinEdges(std::vector<Join::JoinEdgePtr> joinEdges) {
        std::stringstream ss;
        ss << "The QueryPlan holds the following JoinEdge(s): \n";
        for (auto joinEdge : joinEdges){
            ss << "\n" << joinEdge->toString();
        }
        return ss.str();
    }

    void JoinOrderOptimizationRule::setCosts(AbstractJoinPlanOperatorPtr plan){
        AbstractJoinPlanOperatorPtr leftOperator = plan->getLeftChild();
        AbstractJoinPlanOperatorPtr rightOperator = plan->getRightChild();

        // Operator Cost : Cardinality after Join
        plan->setOperatorCosts((leftOperator->getCardinality() * rightOperator->getCardinality()) * plan->getSelectivity());
        plan->setCardinality(plan->getOperatorCosts());

        // CumulativeCosts : OP Cost + children's cumulative costs.
        plan->setCumulativeCosts(plan->getOperatorCosts() + leftOperator->getCumulativeCosts() + rightOperator->getCumulativeCosts());
    }


    std::vector<std::vector<int>> JoinOrderOptimizationRule::getCountCombs(int level){
        std::vector<std::vector<int>> combs;
        for (int i = 0; (2 * i) <= (level + 1); i++)  { // 0, 1, 2
            std::vector<int> combination;
            combination.push_back(i + 1); // 1, 2, 3
            combination.push_back(level - i); // 3, 2, 1
            combs.push_back(combination); // [1, 2], [2, 2], [3,2]
        }
        // remove combs and return
        combs.pop_back();
        return combs;
    }

    std::optional<AbstractJoinPlanOperatorPtr> JoinOrderOptimizationRule::join(AbstractJoinPlanOperatorPtr left, AbstractJoinPlanOperatorPtr right, std::vector<Join::JoinEdgePtr> joins){
        //Get for left and right the involved OptimizerPlanOperators (logical data streams, potentially joined)
        std::set<OptimizerPlanOperatorPtr> leftInvolved = left->getInvolvedOptimizerPlanOperators();
        std::set<OptimizerPlanOperatorPtr> rightInvolved = right->getInvolvedOptimizerPlanOperators();

        //Initialize finalPred which is used to combine multiple join predicates together
        //Important as plans may be joined with multiple possible join graph edges
        Join::JoinEdgePtr joinEdge = nullptr;

        //Iterates over all join edges and updates the finalPred
        for (Join::JoinEdgePtr edge : joins) {
            if (isIn(leftInvolved, rightInvolved, edge) || isIn(rightInvolved, leftInvolved, edge)) {
                joinEdge = edge;
            }
        }

        //In case no join edge was found the finalPred is still null and no link
        //between the left and right plan exists therefore we can not join them and return
        if (joinEdge == nullptr) return std::nullopt;
        else {
            return std::optional<AbstractJoinPlanOperatorPtr>(std::make_shared<AbstractJoinPlanOperator>(left,
                                                                                                         right,
                                                                                                         joinEdge->getJoinDefinition(),
                                                                                                         joinEdge->getSelectivity()));
        }
    }

    bool JoinOrderOptimizationRule::isIn(std::set<OptimizerPlanOperatorPtr> leftInvolved,
                                         std::set<OptimizerPlanOperatorPtr> rightInvolved,
                                         Join::JoinEdgePtr edge) {

        auto leftChild = edge->getLeftOperator();
        auto rightChild = edge->getRightOperator();

        bool inLeft = false;
        bool inRight = false;

        for (auto leftInvolvedOperator : leftInvolved){
            if (leftInvolvedOperator->getId() == leftChild->getId()){
                inLeft = true;
            }
        }
        for (auto rightInvolvedOperator : rightInvolved){
            if (rightInvolvedOperator->getId() == rightChild->getId()){
                inRight = true;
            }
        }
        // check overlap
        bool noOverlapping = true;
        for (auto optimizerPlanOperatorLeft : leftInvolved) {
            if (rightInvolved.find(optimizerPlanOperatorLeft) != rightInvolved.end()){
                noOverlapping = false;
                break;
            }
        }
        return inLeft && inRight && noOverlapping;
    }
    // NOTE: This is completely hardcoded. It would be better to have a singleton class here that takes care of all cases
    int JoinOrderOptimizationRule::getHardCodedCardinalitiesForSource(OptimizerPlanOperatorPtr source) {
            auto name = source->getSourceNode()->getSourceDescriptor()->getLogicalSourceName();
                if (name == "NATION")
                    return 25;
                else if (name == "REGION")
                    return 1;
                else if (name == "SUPPLIER")
                    return 200;
                else if (name == "PARTSUPPLIER")
                    return 20;
                else{
                    return 500;
                }
        }
    void JoinOrderOptimizationRule::getHardCodedJoinSelectivities(std::vector<Join::JoinEdgePtr> joinEdges) {
        for (Join::JoinEdgePtr edge : joinEdges){
            auto leftName = edge->getLeftOperator()->getSourceNode()->getSourceDescriptor()->getLogicalSourceName();
            auto rightName = edge->getRightOperator()->getSourceNode()->getSourceDescriptor()->getLogicalSourceName();
            if ((leftName == "SUPPLIER" && rightName == "PARTSUPPLIER") || (rightName == "SUPPLIER" && leftName == "PARTSUPPLIER"))
                edge->setSelectivity(0.005);
            else if ((leftName == "SUPPLIER" && rightName == "NATION") || (rightName == "SUPPLIER" && leftName == "NATION"))
                edge->setSelectivity(0.04);
            else if ((leftName == "REGION" && rightName == "NATION") || (rightName == "REGION" && leftName == "NATION"))
                edge->setSelectivity(0.2);
            else
                edge->setSelectivity(1);
        }
    }

    std::any JoinOrderOptimizationRule::extractJoinOrder(AbstractJoinPlanOperatorPtr root) {
       // std::vector<std::any> joinOrder;
        if (root->getInvolvedOptimizerPlanOperators().size() == 1){
            return root->getSourceNode()->getId();
        } else{
            std::vector<std::any> subSteps;
            subSteps.push_back(extractJoinOrder(root->getLeftChild()));
            subSteps.push_back(extractJoinOrder(root->getRightChild()));
            return subSteps;
        }
    }


    // Builds a new query plan based on the updated joinOrder.
    QueryPlanPtr JoinOrderOptimizationRule::updateJoinOrder(QueryPlanPtr oldPlan,
                                                            AbstractJoinPlanOperatorPtr finalPlan,
                                                            const std::vector<SourceLogicalOperatorNodePtr> sourceOperators) {
        // copy current plan
        QueryPlanPtr newPlan = oldPlan.get()->copy();

        // Check if we have a finalPlan
        JoinLogicalOperatorNodePtr topJoin;
        if (finalPlan != nullptr){
            // additional check!
            if (finalPlan->getLeftChild() == nullptr && finalPlan->getRightChild() == nullptr){
                return oldPlan;
            }
            // Recursively construct joins based on Children nodes.
            topJoin = constructJoin(finalPlan->getLeftChild(),
                                    finalPlan->getRightChild(),
                                    finalPlan->getJoinPredicate(),
                                    sourceOperators);
        } else{
            NES_DEBUG("FinalPlan is nullptr - returning old plan")
            return oldPlan;
        }

        newPlan->replaceRootOperator(newPlan->getRootOperators()[0], topJoin);
        NES_DEBUG(newPlan->toString());

        return newPlan;

    }

    // simple join order print method, which will show the proposed "optimal" join order.
    std::string JoinOrderOptimizationRule::printJoinOrder(std::any joinOrder) {
        std::stringstream ss;
        if (joinOrder.type() == typeid(std::vector<std::any>)){
            ss << "Joining:\n";
            // cast to std::vector<any>
            std::vector<std::any> elements = std::any_cast<std::vector<std::any>>(joinOrder);
            for(auto el : elements){
            ss << printJoinOrder(el);
            }
        } else if (joinOrder.type() == typeid(u_int64_t)){
            ss << "Source detected: " << std::any_cast<u_int64_t>(joinOrder) << "\n";
        }

        return ss.str();


    }

    // Method recursively constructs join operators
    JoinLogicalOperatorNodePtr JoinOrderOptimizationRule::constructJoin(const AbstractJoinPlanOperatorPtr& leftChild,
                                                                        const AbstractJoinPlanOperatorPtr& rightChild,
                                                                        Join::LogicalJoinDefinitionPtr joinDefinition,
                                                                        const std::vector<SourceLogicalOperatorNodePtr> sources) {




        // Check if we are dealing with a source OR a join respectively for left and right child
        // leftChild
        SourceLogicalOperatorNodePtr leftSourceNode; // If leftChild == source, find corresponding SourceLogicalOperatorNodePtr in sources array
        JoinLogicalOperatorNodePtr leftJoinNode; // if leftChild == join, construct a join by invoked a recursion with its children.
        if (leftChild->getSourceNode()!=nullptr){
            // source
            for (auto source: sources){
                // iterate over sources to find matching source (by id)
                if(leftChild->getSourceNode()->getId() == source->getId()){
                    leftSourceNode = source;
                    break;
                }
            }
        } else{
            // join
            leftJoinNode = constructJoin(leftChild->getLeftChild(),
                                         leftChild->getRightChild(),
                                         leftChild->getJoinPredicate(),
                                         sources);
        }

        // rightChild
        SourceLogicalOperatorNodePtr rightSourceNode;
        JoinLogicalOperatorNodePtr rightJoinNode;
        if(rightChild->getSourceNode()!=nullptr){
            // source
            for(auto source: sources){
                if(rightChild->getSourceNode()->getId() == source->getId()){
                    rightSourceNode = source;
                    break;
                }
            }
        }else{
            // join
            rightJoinNode = constructJoin(rightChild->getLeftChild(),
                                          rightChild->getRightChild(),
                                          rightChild->getJoinPredicate(),
                                          sources);
        }

        // List all 4 cases possible and construct respective JoinLogicalOperatorNode
        if(leftSourceNode != nullptr && rightSourceNode != nullptr){
            NES_DEBUG("Dealing with two sources: " << leftSourceNode->getSourceDescriptor()->getLogicalSourceName() << " and " << rightSourceNode->getSourceDescriptor()->getLogicalSourceName());
            // both sources
            JoinLogicalOperatorNodePtr join = std::make_shared<JoinLogicalOperatorNode>(JoinLogicalOperatorNode(joinDefinition, Util::getNextOperatorId()));


            // add left and right source as children to newly constructed Join.
            // these sources may have additional parent nodes like a watermark assigner or a filter. by calling getPotentialParentNodes()
            // we retrieve them and return the parent the furthest ancestor node before the join
            join->addChild(getPotentialParentNodes(leftSourceNode ));
            join->addChild(getPotentialParentNodes(rightSourceNode));

            return join;

        }
        else if(leftJoinNode != nullptr && rightJoinNode != nullptr){
            NES_DEBUG("Dealing with two joins. On the left we join fields : " << leftJoinNode->getJoinDefinition()->getLeftJoinKey()->getFieldName() << " and " << leftJoinNode->getJoinDefinition()->getRightJoinKey()->getFieldName());
            NES_DEBUG("On The right we join field: " << rightJoinNode->getJoinDefinition()->getLeftJoinKey()->getFieldName() << " and " << rightJoinNode->getJoinDefinition()->getRightJoinKey()->getFieldName())
            // both joins
            // join of joins...
            JoinLogicalOperatorNodePtr join = std::make_shared<JoinLogicalOperatorNode>(JoinLogicalOperatorNode(joinDefinition, Util::getNextOperatorId()));

            // add left and right join to this.
            join->addChild(leftJoinNode);
            join->addChild(rightJoinNode);

            return join;


        }
            // left Join, right Source
        else if(leftJoinNode != nullptr && rightSourceNode != nullptr){
            NES_DEBUG("Joining a left join and a right source");
            NES_DEBUG("On the left we join with the product of join : " << leftJoinNode->getJoinDefinition()->getLeftJoinKey()->getFieldName() << " and " << leftJoinNode->getJoinDefinition()->getRightJoinKey()->getFieldName());
            NES_DEBUG("On the right we join with " << rightSourceNode->getSourceDescriptor()->getLogicalSourceName());

            JoinLogicalOperatorNodePtr join = std::make_shared<JoinLogicalOperatorNode>(JoinLogicalOperatorNode(joinDefinition, Util::getNextOperatorId()));

            // add left join and right source to this
            join->addChild(leftJoinNode);
            join->addChild(getPotentialParentNodes(rightSourceNode));

            return join;


        }
        // left Source, right join
        else if (leftSourceNode != nullptr && rightJoinNode != nullptr){
            NES_DEBUG("Joining a left source and a right join");
            NES_DEBUG("On the left we join with " << leftSourceNode->getSourceDescriptor()->getLogicalSourceName());
            NES_DEBUG("On the right we join with the product of join : " << rightJoinNode->getJoinDefinition()->getLeftJoinKey()->getFieldName() << " and " << rightJoinNode->getJoinDefinition()->getRightJoinKey()->getFieldName());

            JoinLogicalOperatorNodePtr join = std::make_shared<JoinLogicalOperatorNode>(JoinLogicalOperatorNode(joinDefinition, Util::getNextOperatorId()));

            // add left source and right join to this;
            join->addChild(getPotentialParentNodes(leftSourceNode));
            join->addChild(rightJoinNode);

            return join;
        }
        else {
            NES_ERROR("Join Children neither sources nor joins. Cannot optimize.");
        }

        return NES::JoinLogicalOperatorNodePtr();
    }

    NodePtr JoinOrderOptimizationRule::getPotentialParentNodes(NodePtr sharedPtr) {

        NodePtr currentNodePtr = sharedPtr;
        auto parents = currentNodePtr->getParents();
        while(!parents[0]->instanceOf<JoinLogicalOperatorNode>()){
            // as long as the currentNode is not a join replace it with the parent node.
            currentNodePtr = parents[0]; // set currentNode to parentNode
            parents = currentNodePtr->getParents(); // next parents
        }
        return currentNodePtr;
    }
    }// namespace NES::Optimizer

