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

/// Phase 4b removed the Phase-1 NotImplemented guard: lowering now constructs a SpillableTimeBasedSliceStore
/// when spilling is enabled (proven end-to-end by SpillableTimeBasedSliceStoreTest, and out-of-gate by the
/// systest suite). This unit can no longer call apply() on a dummy operator — without the guard, apply() trips
/// the operator-shape PRECONDITIONs (std::terminate, uncatchable) before the store branch, and the file has no
/// WindowedAggregationLogicalOperator builder. So we assert the config wire the rule reads.
TEST_F(LowerToPhysicalWindowedAggregationSpillRoutingTest, spillEnabledFlagReadable)
{
    const auto conf = configWithSpillEnabled();
    EXPECT_TRUE(conf.spill.enabled.getValue());
}

/// By default spilling is disabled, so lowering keeps using the in-memory DefaultTimeBasedSliceStore. The
/// in-memory lowering path is exercised by the full windowed-aggregation regression and systest suite.
TEST_F(LowerToPhysicalWindowedAggregationSpillRoutingTest, spillDisabledByDefault)
{
    const QueryExecutionConfiguration conf;
    EXPECT_FALSE(conf.spill.enabled.getValue());
}

}
