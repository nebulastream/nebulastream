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
            NES_DEBUG("Found a total of " << joinOperators.size() << " JoinOperators. Trying to evaluate best join order.");

            // SourceOperators are the logical data streams that are similar to relations in DBMS
            const auto sourceOperators = queryPlan->getSourceOperators();

            std::vector<OptimizerPlanOperatorPtr> sources;
            for (const auto &source : sourceOperators){
                sources.push_back(std::make_shared<OptimizerPlanOperator>(source));
            }
            getHardCodedCardinalitiesForSources(sources);

            //  Derive a join graph by getting all the join edges between the sources (logical streams)
            std::vector<Join::JoinEdgePtr> joinEdges = retrieveJoinEdges(joinOperators, sources);
            getHardCodedJoinSelectivities(joinEdges);

            // print joinEdges
            NES_DEBUG(listJoinEdges(joinEdges));

            // JVS: Step 5: Derive the best order of joins
            OptimizerPlanOperatorPtr finalPlan = optimizeJoinOrder(sources,joinEdges);


            // JVS: Step 6 Rewrite the query according to the best order


            return queryPlan;

        } else {
            NES_DEBUG("Not enough JoinOperator found to do JoinReordering. Amount of joinOperations: " << joinOperators.size());
            return queryPlan;
        }
    }

    // we need to make sure, that, when we deriveSourceName, we do not query the sourceOperators again, but rather query our list of OptimizerPlanOperators for that exact object
    // with that log. streamName. This would then allows us to use to declare it only once and don't screw up lvl 2.
    std::vector<Join::JoinEdgePtr> JoinOrderOptimizationRule::retrieveJoinEdges(std::vector<JoinLogicalOperatorNodePtr> joinOperators, std::vector<OptimizerPlanOperatorPtr> sources){

        // Iterate over joinOperators and detect all JoinEdges
        std::vector<Join::JoinEdgePtr> joinEdges;
        for (auto join : joinOperators){

            // JVS might be too hacky... at least handle if joinEdge can't be constructed.
            // Construct joinEdge
            std::string leftSource = deriveSourceName(join->getJoinDefinition()->getLeftJoinKey());
            std::string rightSource = deriveSourceName(join->getJoinDefinition()->getRightJoinKey());


            OptimizerPlanOperatorPtr leftOperatorNode;
            OptimizerPlanOperatorPtr rightOperatorNode;

            for (auto optimizerPlanOperator : sources){

                if(leftSource == optimizerPlanOperator->getSourceNode()->getSourceDescriptor()->getLogicalSourceName()){
                    leftOperatorNode = optimizerPlanOperator;
                }
                if (rightSource == optimizerPlanOperator->getSourceNode()->getSourceDescriptor()->getLogicalSourceName()){
                    rightOperatorNode = optimizerPlanOperator;
                }
            }

            NES_DEBUG("Found the following corresponding SourceLogicalOperatorNodes: " << leftOperatorNode->getSourceNode()->toString() << " and " << rightOperatorNode->getSourceNode()->toString());

            Join::JoinEdge joinEdge = Join::JoinEdge(leftOperatorNode, rightOperatorNode, join->getJoinDefinition(), 0.5);
            joinEdges.push_back(std::make_shared<Join::JoinEdge>(joinEdge));
        }

        return joinEdges;

    }

    // optimizing the join order given the join graph
    OptimizerPlanOperatorPtr JoinOrderOptimizationRule::optimizeJoinOrder(std::vector<OptimizerPlanOperatorPtr> sources, std::vector<Join::JoinEdgePtr> joins){
        //For each key (number of joined relations) the HashMap includes a HashMap where for each possible sub-combination of joins
        // The optimal one is stored (exists only if it is possible using JoinPredicates)
        std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>> subs;

        //Estimate the cardinality for all relations and add them in the HashMap
        // adding empty map for int 1
        subs[1] = std::map<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>();
        NES_DEBUG("Setting the base logical streams for Optimization table: ")

        for (auto source : sources){
            source->setOperatorCosts(0);
            //source->setCumulativeCosts(source->getCardinality());
            // insert rows into first level of hashmap
            // the key is a set of already joined relations (involvedOptimizerPlanOperators) and the value is the to be joined source.
            std::set<OptimizerPlanOperatorPtr> key;
            key.insert(source);
            source->setInvolvedOptimizerPlanOperators(key);
//            subs[1][key] = source;
            subs[1].insert(std::pair<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>(key, source));
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
        OptimizerPlanOperatorPtr finalPlan = subs[sources.size()][sourceSet];
        // JVS Build OptimizerPlanOperator to String method.
        //NES_DEBUG("Optimal Plan received " << finalPlan.toString())
        return finalPlan;

    }

    std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>> JoinOrderOptimizationRule::createJoinCandidates(std::map<int, std::map<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>> subs, int level, std::vector<Join::JoinEdgePtr> joins){
        //Add HashMap for current level (number of involved relations)
        // für das nächste Level eine Schicht
        subs[level] = std::map<std::set<OptimizerPlanOperatorPtr>, OptimizerPlanOperatorPtr>();

        //Create relation pairs and add them into the 2-level HashMap
        //(initial step therefore special case)
        if (level == 2) {
            for (auto graphEdge : joins) {
                // as these are two regular streams that are being joint, getting leftSource instead of leftOperator is fine.
                AbstractJoinPlanOperatorPtr plan = std::make_shared<AbstractJoinPlanOperator>(graphEdge->getLeftOperator(), graphEdge->getRightOperator(), graphEdge->getJoinDefinition());
                setCosts(plan);
                subs[2][plan->getInvolvedOptimizerPlanOperators1()] = plan; // JVS test if this works with pointers as set. might need the actual values here.
            }
            return subs;
        }
        //Case for all levels>2
        //Get possible combinations (for a join of 3 relations => only 1 Relation joins 2 already Joined relations possible)
        std::vector<std::vector<int>> combs = getCountCombs(level - 1); // TEST!!!!
                                                                       //Create for each possible combination of joins an AbstractJoinPlanOperator
        //Should a plan with an identical relation combination already exists the cost-efficient
        //plan is added to the level-HashMap
        for (auto comb : combs) {
            // left side of comb is comb[0] and right side is comb[1] as we are dealing with pairs.
            for(auto left : subs[comb[0]]){
                for (auto right : subs[comb[1]]){
                    // create a new AbstractJoinPlanOperator for each combination therefore and set its costs.
                    std::optional<AbstractJoinPlanOperatorPtr> newPlan = join(left.second, right.second, joins);
                    if (newPlan){
                        setCosts(newPlan.value());
                        // check if newPlan is currently the best
                        // note: if an empty OptimizerPlanOperator is made from belows statement, the operatorCosts of that plan are set to -1
                        OptimizerPlanOperatorPtr oldPlan = subs[level][newPlan.value()->getInvolvedOptimizerPlanOperators()]; // this returns for the very same logicalStreams the current best plan

                        if (oldPlan == nullptr  || oldPlan->getCumulativeCosts() > newPlan.value()->getCumulativeCosts()){
                            subs[level][newPlan.value()->getInvolvedOptimizerPlanOperators1()] = newPlan.value();
                        }
                    }
                }
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
        // JVS hier müsste der globale Card. Estimator greifen.
        OptimizerPlanOperatorPtr leftOperator = plan->getLeftChild();
        OptimizerPlanOperatorPtr rightOperator = plan->getRightChild();
        plan->setOperatorCosts(leftOperator->getCardinality() + rightOperator->getCardinality());
        plan->setCumulativeCosts(plan->getOperatorCosts() + leftOperator->getCumulativeCosts() + rightOperator->getCumulativeCosts());
    }

    std::vector<std::vector<int>> JoinOrderOptimizationRule::getCountCombs(int level){
        // try with level 4 --> 4-1 = 3
        // level == 3
        std::vector<std::vector<int>> combs;
        for (int i = 0; (2 * i) <= (level + 1); i++)  { // 0, 1, 2
            std::vector<int> combination;
            combination.push_back(i + 1); // 1, 2, 3
            combination.push_back(level - 1); // 2, 2, 2
            combs.push_back(combination); // [1, 2], [2, 2], [3,2]
        }
        // remove combs and return
        combs.pop_back(); // check if this really does combs.remove(combs.get(combs.size() - 1)); [1,1]; [1, 2], [2,2]
        return combs;
    }
    // JVS test this method. I need to know whether this is a good/working way to construct the join tree.
    std::optional<AbstractJoinPlanOperatorPtr> JoinOrderOptimizationRule::join(OptimizerPlanOperatorPtr left, OptimizerPlanOperatorPtr right, std::vector<Join::JoinEdgePtr> joins){
        //Get for left and right the involved OptimizerPlanOperators (logical data streams, potentially joined)
        std::set<OptimizerPlanOperatorPtr> leftInvolved = left->getInvolvedOptimizerPlanOperators();
        std::set<OptimizerPlanOperatorPtr> rightInvolved = right->getInvolvedOptimizerPlanOperators();


        //Initialize finalPred which is used to combine multiple join predicates together
        //Important as plans may be joined with multiple possible join graph edges
        std::vector<Join::LogicalJoinDefinitionPtr> finalPred;

        //Iterates over all join edges and updates the finalPred
        for (Join::JoinEdgePtr edge : joins) {
            if (isIn(leftInvolved, rightInvolved, edge)) {
                finalPred.push_back(edge->getJoinDefinition());
            }
        }

        //In case no join edge was found the finalPred is still null and no link
        //between the left and right plan exists therefore we can not join them and return
        //null, else we return the newly created joined plan
        if (finalPred.size() == 0) return std::nullopt; // this is when there is no edge
        else {
            // JVS was mache ich jetzt mit der liste um einen AbstractJoinPlanOperator herzustellen?
            // das könnte hier falsch sein. Meine Logik ist: du hast links und rechts die bereits gejoint sind.
            // jetzt nimmst du einfach ein neues prädikat und joinst auf dem nächsten level.
            // Die Frage ist, wie ich denn jetzt die Order richtig ableite. Die könnte ich ja ggf. als left, right jeweils Kinder ableiten.

            // JVS also das ist jetzt denke ich sicher falsch gelöst.
            // Müsste mir das nochmal anschauen, sobald ich joinOrderTesten kann.
            return std::optional<AbstractJoinPlanOperatorPtr>(std::make_shared<AbstractJoinPlanOperator>(AbstractJoinPlanOperator(left, right, finalPred[0])));
        }
    }

    // JVS see if this works as we are only doing this with pointers instead of the real objects.
    bool JoinOrderOptimizationRule::isIn(std::set<OptimizerPlanOperatorPtr> leftInvolved,
                                         std::set<OptimizerPlanOperatorPtr> rightInvolved,
                                         Join::JoinEdgePtr edge) {
        auto leftChild = edge->getLeftOperator();
        auto rightChild = edge->getRightOperator();

        // check if edge left or right is equal to leftChild/rightChild -- need to transform rightChild to OptimizerPlanOperator mb.
        const bool inLeft = (leftInvolved.find(leftChild) != leftInvolved.end()) || (leftInvolved.find(rightChild) != leftInvolved.end());
        const bool inRight = (rightInvolved.find(leftChild) != rightInvolved.end()) || (rightInvolved.find(rightChild) != rightInvolved.end());

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
    void JoinOrderOptimizationRule::getHardCodedCardinalitiesForSources(std::vector<OptimizerPlanOperatorPtr> sources) {
        for (OptimizerPlanOperatorPtr source : sources){
            auto name = source->getSourceNode()->getSourceDescriptor()->getLogicalSourceName();
                if (name == "NATION")
                    source->setCardinality(25);
                else if (name == "REGION")
                    source->setCardinality(1);
                else if (name == "SUPPLIER")
                    source->setCardinality(200);
                else if (name == "PARTSUPPLIER")
                    source->setCardinality(20);
                else{
                    source->setCardinality(500);
                }
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

    }// namespace NES::Optimizer

