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

#include <utility>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>

namespace NES
{
namespace
{
class RedundantProjectionRemovalRuleTest : public Testing::BaseUnitTest
{
protected:
    OptimizerTestUtils utils;
};

TEST_F(RedundantProjectionRemovalRuleTest, RemovesOrderPreservingIdentityProjection)
{
    auto source = utils.createSource("source", {"sample", "timestamp"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("sample"), FieldAccessLogicalFunction{source->getOutputSchema()[Identifier::parse("sample")].value()}},
            {Identifier::parse("timestamp"),
             FieldAccessLogicalFunction{source->getOutputSchema()[Identifier::parse("timestamp")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    const auto optimized
        = RedundantProjectionRemovalRule{}.apply(utils.createPlan(utils.createSink(projection, "sink", {"sample", "timestamp"})));
    EXPECT_TRUE(optimized.getRootOperators().front().getChildren().front().tryGetAs<SourceDescriptorLogicalOperator>());
}

TEST_F(RedundantProjectionRemovalRuleTest, RetainsIdentityProjectionThatReordersFields)
{
    auto source = utils.createSource("source", {"sample", "timestamp"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("timestamp"), FieldAccessLogicalFunction{source->getOutputSchema()[Identifier::parse("timestamp")].value()}},
            {Identifier::parse("sample"), FieldAccessLogicalFunction{source->getOutputSchema()[Identifier::parse("sample")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});

    const auto optimized
        = RedundantProjectionRemovalRule{}.apply(utils.createPlan(utils.createSink(projection, "sink", {"timestamp", "sample"})));
    EXPECT_TRUE(optimized.getRootOperators().front().getChildren().front().tryGetAs<ProjectionLogicalOperator>());
}
}
}
