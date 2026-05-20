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

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES::Testing
{

namespace
{
QueryExecutionConfiguration configWithSpillEnabled()
{
    QueryExecutionConfiguration conf;
    conf.overwriteConfigWithCommandLineInput({{"spill.enabled", "true"}});
    return conf;
}
}

class LowerToPhysicalWindowedAggregationSpillRoutingTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("LowerToPhysicalWindowedAggregationSpillRoutingTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup LowerToPhysicalWindowedAggregationSpillRoutingTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// Load-bearing Phase 1 assertion: when spilling is requested, lowering refuses with
/// NotImplemented. The guard is the first statement of apply(), so it fires before the
/// operator-shape preconditions — proving the new config wire reaches the lowering rule.
TEST_F(LowerToPhysicalWindowedAggregationSpillRoutingTest, spillEnabledThrowsNotImplemented)
{
    LowerToPhysicalWindowedAggregation rule{configWithSpillEnabled()};
    const LogicalOperator dummyOperator{SourceNameLogicalOperator{"dummy"}};

    ASSERT_EXCEPTION_ERRORCODE(rule.apply(dummyOperator), ErrorCode::NotImplemented);
}

/// The refusal is keyed on configuration, not on operator shape: a different operator
/// still throws NotImplemented when spilling is enabled.
TEST_F(LowerToPhysicalWindowedAggregationSpillRoutingTest, spillEnabledThrowsRegardlessOfOperator)
{
    LowerToPhysicalWindowedAggregation rule{configWithSpillEnabled()};
    const LogicalOperator selectionOperator{SelectionLogicalOperator{FieldAccessLogicalFunction{"field"}}};

    ASSERT_EXCEPTION_ERRORCODE(rule.apply(selectionOperator), ErrorCode::NotImplemented);
}

/// By default spilling is disabled, so the guard is never taken and lowering keeps using the
/// in-memory DefaultTimeBasedSliceStore. The actual in-memory lowering path is exercised by the
/// full windowed-aggregation regression and systest suite (all of which run with spill disabled);
/// calling apply() here with a non-aggregation operator would trip a PRECONDITION (std::terminate),
/// so we assert the guard predicate the rule reads.
TEST_F(LowerToPhysicalWindowedAggregationSpillRoutingTest, spillDisabledByDefault)
{
    const QueryExecutionConfiguration conf;
    EXPECT_FALSE(conf.spill.enabled.getValue());
}

}
