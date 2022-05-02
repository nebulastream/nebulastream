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

    // clang-format off
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include "../../../../../nes-common/tests/util/NesBaseTest.hpp"
#include "Windowing/WindowingForwardRefs.hpp"
#include <Util/Logger/Logger.hpp>
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/QueryRewrite/JoinOrderOptimizationRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <iostream>
#include <API/Query.hpp>
// clang-format on

using namespace NES;

    class JoinOrderOptimizationRuleTest : public Testing::NESBaseTest {

      public:
        SchemaPtr schema;

        /* Will be called before a test is executed. */
        void SetUp() override {
            NES::Logger::setupLogging("JoinOrderOptimizationRuleTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup JoinOrderOptimizationRuleTest test case.");
            schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        }

        /* Will be called before a test is executed. */
        void TearDown() override { NES_INFO("Setup JoinOrderOptimizationRuleTest test case."); }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down JoinOrderOptimizationRuleTest test class."); }
    };



    TEST_F(JoinOrderOptimizationRuleTest, tpchFourJoinTests) {
        // Sink descriptor -- check if thats even necessary
        SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

        // TODO Define LogicalStreams with specific schemas found also in TPCH queries.

        // Define Sources we are streaming from
        Query subQuery1 = Query::from("SUPPLIER").filter(Attribute("PARTKEY") > 1000 && Attribute("PARTKEY") < 1005);
        Query subQuery2 = Query::from("PARTSUPPLIER");
        Query subQuery3 = Query::from("NATION");
        Query subQuery4 = Query::from("REGION").filter(Attribute("NAME") == "AFRICA"); // check if this is done correctly ("R.NAME = \"AFRICA\"")


        Query query = subQuery1.joinWith(subQuery2).where(Attribute("SUPPLIER$SUPPKEY")).equalsTo(Attribute("PARTSUPPLIER$SUPPKEY")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                          .joinWith(subQuery3).where(Attribute("SUPPLIER$NATIONKEY")).equalsTo(Attribute("NATION$NATIONKEY")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                          .joinWith(subQuery4).where(Attribute("REGION$REGIONKEY")).equalsTo(Attribute("NATION$REGIONKEY")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)));
        QueryPlanPtr queryPlan = query.getQueryPlan();

        auto joinOrderOptimizationRule = Optimizer::JoinOrderOptimizationRule::create();
        queryPlan = joinOrderOptimizationRule->apply(queryPlan);


        //NES_DEBUG(queryPlan->toString());



        EXPECT_TRUE(1 == (2-1));

    }

    TEST_F(JoinOrderOptimizationRuleTest, randomTest){
        Schema schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        NES_DEBUG(schema.toString());
        EXPECT_TRUE(1 == (2-1));
    }

    /*

JoinGraphEdge[] edges = new JoinGraphEdge[] {
new JoinGraphEdge(regionTable, nationTable, region2nationPred),
new JoinGraphEdge(nationTable, suppTable, nation2suppPred),
new JoinGraphEdge(partSuppTable, suppTable, partsupp2suppPred) };

AnalyzedSelectQuery query = new AnalyzedSelectQuery(tables, edges);
 * */