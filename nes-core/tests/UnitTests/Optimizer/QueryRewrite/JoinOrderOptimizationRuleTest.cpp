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
#include <Plans/Query/QueryPlan.hpp>
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

    // Q1
    TEST_F(JoinOrderOptimizationRuleTest, sequencePattern){

        // TODO: Define Pattern as Query
        Query subQuery1 = Query::from("Velocity").filter(Attribute("value") > 175);
        Query subQuery2 = Query::from("Quantity").filter(Attribute("value") > 250);
        Query query = subQuery1.seqWith(subQuery2).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)));

        std::string outputFilePath = getTestResourceFolder() / "testSmallSequencePattern.out";


        std::string queryStr = R"(Query::from("Quantity").filter(Attribute("value") > 175).seqWith(Query::from("Quantity").filter(Attribute("value") > 250))
.window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10))).sink(FileSinkDescriptor::create(")"
            + outputFilePath + "\"));";

        EXPECT_TRUE(1 == (2-1));

    }

    TEST_F(JoinOrderOptimizationRuleTest, longSequencePattern){
        Query subQuery1 = Query::from("Velocity").filter(Attribute("value") > 50);
        Query subQuery2 = Query::from("Quantity").filter(Attribute("value") > 3);
        Query subQuery3 = Query::from("PM25").filter(Attribute("value") > 5);
        Query subQuery4 = Query::from("PM10").filter(Attribute("value") > 6);
        Query subQuery5 = Query::from("Temperature").filter(Attribute("value") > 13);
        Query subQuery6 = Query::from("Humidity").filter(Attribute("value") > 6);

        Query query = subQuery1.seqWith(subQuery2).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)))
            .seqWith(subQuery3).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)))
            .seqWith(subQuery4).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)))
            .seqWith(subQuery5).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)))
            .seqWith(subQuery6).window(TumblingWindow::of(EventTime(Attribute("ts")), Minutes(10)));

        QueryPlanPtr queryPlan = query.getQueryPlan();
        NES_DEBUG(queryPlan->toString());

        EXPECT_TRUE(1 == (2-1));

    }


    TEST_F(JoinOrderOptimizationRuleTest, sequencePattern4Streams){
/*        Query subQuery1 = Query::from("Quantity").seqWith(Query::from("Velocity")).window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)));
        Query subQuery2 = Query::from("Temperature").seqWith(Query::from("Humidity")).window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)));
        Query query = subQuery1.seqWith(subQuery2).window(TumblingWindow::of(EventTime(Attribute("peter")), Minutes(5)));
        */

        Query query = Query::from("Quantity").seqWith(Query::from("Velocity")).window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)))
            .seqWith(Query::from("Temperature")).window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)))
            .seqWith(Query::from("Humidity")).window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)));

        QueryPlanPtr queryPlan = query.getQueryPlan();

        NES_DEBUG(queryPlan->toString())

        auto joinOrderOptimizationRule = Optimizer::JoinOrderOptimizationRule::create();
        queryPlan = joinOrderOptimizationRule->apply(queryPlan);

        NES_DEBUG(queryPlan->toString())

    }

    TEST_F(JoinOrderOptimizationRuleTest, sequencePattern4StreamsAsJoinsDirectly){
        Query subQuery1 = Query::from("Quantity")
                              .joinWith(Query::from("Velocity"))
                              .where(Attribute("ts")).equalsTo(Attribute("ts"))
                              .window(TumblingWindow::of(EventTime(Attribute("ts")),Minutes(5)))
                              .filter(Attribute("Quantity$ts") < Attribute("Velocity$ts"));


        Query subQuery2 = Query::from("Temperature")
                              .joinWith(Query::from("Humidity"))
                              .where(Attribute("ts")).equalsTo(Attribute("ts"))
                              .window(TumblingWindow::of(
                                  EventTime(Attribute("ts")),Minutes(5))
                                          )
                              .filter(Attribute("Temperature$ts") < Attribute("Humidity$ts"));

        // TODO how can i use left joins bigger time stamp here?
        Query query = subQuery1.joinWith(subQuery2).where(Attribute("ts")).equalsTo(Attribute("ts")).window(TumblingWindow::of(
            EventTime(Attribute("ts")),Minutes(5))).filter(Attribute("Velocity$ts") < Attribute("Temperature$ts"));


           // subQuery1.seqWith(subQuery2).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Minutes(10)));
        QueryPlanPtr queryPlan = query.getQueryPlan();

        NES_DEBUG(queryPlan->toString())

        auto joinOrderOptimizationRule = Optimizer::JoinOrderOptimizationRule::create();
        queryPlan = joinOrderOptimizationRule->apply(queryPlan);

        NES_DEBUG(queryPlan->toString())


    }

    // Q2 -- will probably not be optimized as it is a Cartesian product
    TEST_F(JoinOrderOptimizationRuleTest, conjunctionPattern){

        EXPECT_TRUE(1 == (2-1));
    }

    // Q3 // will not be optimized as this is a union
    TEST_F(JoinOrderOptimizationRuleTest, disjunctionPattern){

        EXPECT_TRUE(1 == (2-1));
    }

    // Q4 -- will not be optimized
    TEST_F(JoinOrderOptimizationRuleTest, iterationPattern){

        EXPECT_TRUE(1 == (2-1));
    }

    // Q5 - will not be optimized
    TEST_F(JoinOrderOptimizationRuleTest, negationPattern){

        EXPECT_TRUE(1 == (2-1));
    }