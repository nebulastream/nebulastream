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

#include <API/Query.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <gtest/gtest.h>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Nodes/Node.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>

namespace NES {

class OperatorPropertiesTest : public testing::Test {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down OperatorPropertiesTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("OperatorPropertiesTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup OperatorPropertiesTest test class.");
    }
};

// test assigning operators properties
TEST_F(OperatorPropertiesTest, testAssignProperties) {
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 1));
    properties.push_back(filterProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 1));
    properties.push_back(sinkProp);

    bool res = UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);

    // Assert if the assignment success
    ASSERT_TRUE(res);

    // Assert if the property are added correctly
    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 1);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 2);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 3);
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf")), 1);
    ++queryPlanIterator;
}

// test assigning different types of operators properties
TEST_F(OperatorPropertiesTest, testAssignDifferentPropertyTypes) {
    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load_int", 1));
    srcProp.insert(std::make_pair("dmf_double", 0.5));
    srcProp.insert(std::make_pair("misc_str", std::string("xyz")));
    properties.push_back(srcProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load_int", 1));
    sinkProp.insert(std::make_pair("dmf_double", 0.5));
    sinkProp.insert(std::make_pair("misc_str", std::string("xyz")));
    properties.push_back(sinkProp);

    bool res = UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);

    // Assert if the assignment success
    ASSERT_TRUE(res);

    // Assert if the property are added correctly
    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load_int")), 1);
    ASSERT_EQ(std::any_cast<double>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf_double")), 0.5);
    ASSERT_EQ(std::any_cast<std::string>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("misc_str")), "xyz");
    ++queryPlanIterator;

    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load_int")), 1);
    ASSERT_EQ(std::any_cast<double>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("dmf_double")), 0.5);
    ASSERT_EQ(std::any_cast<std::string>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("misc_str")), "xyz");
    ++queryPlanIterator;
}

// test on providing more properties than the number of operators in the query
TEST_F(OperatorPropertiesTest, testAssignWithMorePropertiesThanOperators) {
    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 1));
    properties.push_back(filterProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 1));
    properties.push_back(sinkProp);

    // this should return false as properties of all operators has to be supplied if one of them is supplied
    bool res = UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_FALSE(res);
}

// test on providing less properties than the number of operators in the query
TEST_F(OperatorPropertiesTest, testAssignWithLessPropertiesThanOperators) {
    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", "1"));
    srcProp.insert(std::make_pair("dmf", "1"));
    properties.push_back(srcProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", "3"));
    sinkProp.insert(std::make_pair("dmf", "1"));
    properties.push_back(sinkProp);

    // this should return false as properties of all operators has to be supplied if one of them is supplied
    bool res = UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_FALSE(res);
}

// test with binary operator
TEST_F(OperatorPropertiesTest, testAssignWithBinaryOperator) {

    auto subQuery = Query::from("default_logical").filter(Attribute("field_1") <= 10);

    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("id"))
                     .equalsTo(Attribute("id"))
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .sink(PrintSinkDescriptor::create());

    std::vector<std::map<std::string, std::any>> properties;

    // Adding properties of each operator. The order should be the same as used in the QueryPlanIterator
    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    properties.push_back(srcProp);

    // adding property of the source watermark (watermarks are added automatically)
    std::map<std::string, std::any> srcWatermarkProp;
    srcWatermarkProp.insert(std::make_pair("load", 2));
    properties.push_back(srcWatermarkProp);

    // adding property of the source in the sub query
    std::map<std::string, std::any> srcSubProp;
    srcSubProp.insert(std::make_pair("load", 3));
    properties.push_back(srcSubProp);

    // adding property of the watermark in the sub query (watermarks are added automatically)
    std::map<std::string, std::any> srcSubWatermarkProp;
    srcSubWatermarkProp.insert(std::make_pair("load", 4));
    properties.push_back(srcSubWatermarkProp);

    // adding property of the filter in the sub query
    std::map<std::string, std::any> filterSubProp;
    filterSubProp.insert(std::make_pair("load", 5));
    properties.push_back(filterSubProp);

    // adding property of the join
    std::map<std::string, std::any> joinProp;
    joinProp.insert(std::make_pair("load", 6));
    properties.push_back(joinProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 7));
    properties.push_back(sinkProp);

    bool res = UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
    ASSERT_TRUE(res);

    auto queryPlanIterator = QueryPlanIterator(query.getQueryPlan()).begin();

    // Assert the property values
    // source (main)
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 1);
    ++queryPlanIterator;
    // source (main) watermark
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 2);
    ++queryPlanIterator;
    // source of subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 3);
    ++queryPlanIterator;
    // watermark of subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 4);
    ++queryPlanIterator;
    // filter in the subquery
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 5);
    ++queryPlanIterator;
    // join
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 6);
    ++queryPlanIterator;
    // sink
    ASSERT_EQ(std::any_cast<int>((*queryPlanIterator)->as<LogicalOperatorNode>()->getProperty("load")), 7);
    ++queryPlanIterator;
}

//
int computeCost(QueryPlanPtr queryPlanPtr, std::vector<int> placement,std::vector<TopologyNodePtr> topology){

    auto queryOperators = QueryPlanIterator(queryPlanPtr).snapshot();
    std::vector<int> operatorDmf;
    std::vector<int> operatorLoad;

    int numberOfOperators = queryOperators.size();
    auto current = queryOperators.begin();

    //Get the dmf and load of each operator and save it to the corresponding vector
    for(int i =0; i < numberOfOperators; i++) {
        operatorDmf.insert(operatorDmf.begin(), std::any_cast<int> ((*current)->as<LogicalOperatorNode>()->getProperty("dmf") ));
        operatorLoad.insert(operatorLoad.begin(),std::any_cast<int> ((*current)->as<LogicalOperatorNode>()->getProperty("load") ) );
        ++current;
    }

    //global cost of nodes in the placement
    std::map<NodePtr,int> globalNodesCost;

    //Iterate over all nodes in the placement to calculate the global cost of each node
    for(int i = 0 ; i < topology.size(); i++){
        int assignment_begin = i*numberOfOperators;
        int localCost = 1;  //if no operators are assigned to the  current node then the local cost is 1
        int nodeLoad = 0;

        //check which operators are assigned to the current node and add the cost of this operators to the local cost of the current node
        for(int j = 0 ; j < numberOfOperators; j++){
            if(placement.at(assignment_begin+j)){
                localCost *= operatorDmf.at(j);
                nodeLoad += operatorLoad.at(j);
            }
        }

        //check wether the current node is overloaded if yes then add the overloading penalization  the local cost
        auto availableCapacity = topology.at(i)->getAvailableResources();
        if(nodeLoad > availableCapacity)
            localCost += (nodeLoad-availableCapacity);

        //Add the global cost of the children nodes to the global cost of the current node
        int childGlobalCost = 0;
        int globalCost = 0;
        auto childern = topology.at(i)->getChildren();
        for (auto& child : childern){
            childGlobalCost = globalNodesCost.at(child);
            // Penalize link overloading and add it to the global cost
            auto linkCapacity = child->as<TopologyNode>()->getLinkProperty(topology.at(i))->bandwidth;
            if(childGlobalCost > linkCapacity)
                globalCost += childGlobalCost - linkCapacity;

            globalCost += localCost * childGlobalCost;

        }
        //In case the current node has no children then the global cost is the local cost
        if(globalCost == 0)
            globalCost = localCost;
        globalNodesCost.insert(std::make_pair(topology.at(i), globalCost));

    }

    // sum up the global cost of the nodes as the overall cost of the placement
    std::map<NodePtr, int>::iterator itr;
    for (itr = globalNodesCost.begin(); itr != globalNodesCost.end(); ++itr) {
        NES_DEBUG("Global Cost of Node "<<itr->first->as<TopologyNode>()->getId() <<": "<<itr->second);

    }
    return globalNodesCost.begin()->second;
}

// test the cost function
TEST_F(OperatorPropertiesTest, testCostFunction) {

auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create());

std::vector<std::map<std::string, std::any>> properties;

// adding property of the source
std::map<std::string, std::any> srcProp;
srcProp.insert(std::make_pair("load", 1));
srcProp.insert(std::make_pair("dmf", 9));
properties.push_back(srcProp);

// adding property of the filter
std::map<std::string, std::any> filterProp;
filterProp.insert(std::make_pair("load", 2));
filterProp.insert(std::make_pair("dmf", 1));
properties.push_back(filterProp);

// adding property of the sink
std::map<std::string, std::any> sinkProp;
sinkProp.insert(std::make_pair("load", 3));
sinkProp.insert(std::make_pair("dmf", 2));
properties.push_back(sinkProp);

UtilityFunctions::assignPropertiesToQueryOperators(query.getQueryPlan(), properties);
NES_DEBUG("Query_Plan: " << query.getQueryPlan()->toString());

// create the topology for the test
TopologyPtr topology = Topology::create();
uint32_t grpcPort = 4000;
uint32_t dataPort = 5000;

TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 4);
topology->setAsRoot(rootNode);
grpcPort++;
dataPort++;
TopologyNodePtr fogNode = TopologyNode::create(2, "localhost", grpcPort, dataPort, 4);
topology->addNewPhysicalNodeAsChild(rootNode, fogNode);
grpcPort++;
dataPort++;
TopologyNodePtr sourceNode1 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 4);
topology->addNewPhysicalNodeAsChild(fogNode, sourceNode1);
grpcPort++;
dataPort++;
TopologyNodePtr sourceNode2 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 4);
topology->addNewPhysicalNodeAsChild(fogNode, sourceNode2);

LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(10, 64));
sourceNode1->addLinkProperty(fogNode, linkProperty1);
fogNode->addLinkProperty(sourceNode1, linkProperty1);

sourceNode2->addLinkProperty(fogNode, linkProperty1);
fogNode->addLinkProperty(sourceNode2, linkProperty1);
LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(100, 512));
fogNode->addLinkProperty(rootNode, linkProperty2);
rootNode->addLinkProperty(fogNode, linkProperty2);

//save placement candidate to calculate the cost
std::vector<int> placement = {0,0,1,0,0,1,0,1,0,1,0,0};
std::vector<TopologyNodePtr> topologyy = {sourceNode1, sourceNode2, fogNode, rootNode};
int cost = NES::computeCost( query.getQueryPlan(), placement, topologyy);
ASSERT_EQ(cost,36);
}

}// namespace NES
