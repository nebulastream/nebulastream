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

#include <Rules/Static/RedundantProjectionRemovalRule.hpp>

#include <optional>
#include <utility>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class RedundantProjectionRemovalRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("RedundantProjectionRemovalRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(RedundantProjectionRemovalRuleTest, RemoveRedundant)
{
    auto source = utils.createSource("removeRedundant", {"a", "b"});

    const std::vector<std::pair<Identifier, LogicalFunction>> projections1{
        {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}}};
    auto projection1 = ProjectionLogicalOperator::create(source, projections1, ProjectionLogicalOperator::Asterisk{false});

    const std::vector<std::pair<Identifier, LogicalFunction>> projections2{
        {Identifier::parse("a"), FieldAccessLogicalFunction{projection1.getOutputSchema()[Identifier::parse("a")].value()}}};
    auto projection2 = ProjectionLogicalOperator::create(projection1, projections2, ProjectionLogicalOperator::Asterisk{false});

    auto sink = utils.createSink(projection2, "removeRedundant", {"a"});

    auto plan = utils.createPlan(sink);

    auto optimized = RedundantProjectionRemovalRule{}.apply(plan);

    auto optSink = optimized.getRootOperators().at(0);
    ASSERT_TRUE(optSink.tryGetAs<SinkLogicalOperator>());
    auto optProj = optSink.getChildren().at(0);
    ASSERT_TRUE(optProj.tryGetAs<ProjectionLogicalOperator>());
    auto optSource = optProj.getChildren().at(0);
    ASSERT_TRUE(optSource.tryGetAs<SourceDescriptorLogicalOperator>());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
