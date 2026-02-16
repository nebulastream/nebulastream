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

#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Util/Reflection.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class ProjectionLogicalOperatorTest : public ::testing::Test
{
protected:
    Schema schema = Schema{}.addField("stream.a", DataType::Type::UINT64).addField("stream.b", DataType::Type::UINT64);
};

/// Two projections with the same explicit alias must be rejected with CannotInferSchema (code 2003).
TEST_F(ProjectionLogicalOperatorTest, DuplicateExplicitAliasThrows)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op{
        std::vector<ProjectionLogicalOperator::Projection>{
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false)};

    ASSERT_EXCEPTION_ERRORCODE((void)op->withInferredSchema({schema}), ErrorCode::CannotInferSchema);
}

/// Two unaliased projections accessing the same field are allowed per standard SQL behavior.
TEST_F(ProjectionLogicalOperatorTest, DuplicateUnaliasedFieldSucceeds)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// An explicit alias that collides with an unaliased field is allowed (only duplicate explicit aliases are rejected).
TEST_F(ProjectionLogicalOperatorTest, AliasCollidingWithUnaliasedFieldSucceeds)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("stream.a"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// Projections with distinct aliases must succeed.
TEST_F(ProjectionLogicalOperatorTest, UniqueAliasesSucceeds)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("y"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// Asterisk plus an unaliased projection that duplicates an input field name is allowed.
TEST_F(ProjectionLogicalOperatorTest, AsteriskPlusDuplicateUnaliasedProjectionSucceeds)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(true));

    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// Known limitation: an explicit alias matching the auto-derived name is treated as auto-derived.
/// SELECT stream.a AS stream.a, stream.b AS stream.a only detects the second alias as explicit.
TEST_F(ProjectionLogicalOperatorTest, ExplicitAliasMatchingAutoDerivedNameTreatedAsAutoDerived)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {FieldIdentifier("stream.a"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("stream.a"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    /// Only one alias differs from the auto-derived name, so no duplicate is detected.
    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// Asterisk plus a distinctly aliased projection must succeed.
TEST_F(ProjectionLogicalOperatorTest, AsteriskPlusNewAliasSucceeds)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {FieldIdentifier("new_a"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(true));

    EXPECT_NO_THROW((void)op->withInferredSchema({schema}));
}

/// Duplicate unaliased projections must survive a reflection round-trip (regression for serialization bug).
/// After the first withInferredSchema, all identifiers are set. The Reflector must distinguish auto-derived
/// from explicit identifiers so that the second withInferredSchema (after deserialization) doesn't falsely
/// reject auto-derived duplicates.
TEST_F(ProjectionLogicalOperatorTest, DuplicateUnaliasedFieldSurvivesReflectionRoundTrip)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    /// First inference (simulates the optimization phase).
    const auto inferred = TypedLogicalOperator<ProjectionLogicalOperator>(op->withInferredSchema({schema}));

    /// Simulate serialization round-trip: reflect → unreflect.
    const auto reflected = NES::reflect(inferred);
    const auto roundTripped = ReflectionContext{}.unreflect<TypedLogicalOperator<ProjectionLogicalOperator>>(reflected);

    /// Second inference (simulates deserialization re-inference) must not throw.
    EXPECT_NO_THROW((void)roundTripped->withInferredSchema({schema}));
}

/// Explicit aliases must still be detected as duplicates after a reflection round-trip.
TEST_F(ProjectionLogicalOperatorTest, DuplicateExplicitAliasSurvivesReflectionRoundTrip)
{
    const TypedLogicalOperator<ProjectionLogicalOperator> op(
        std::vector<ProjectionLogicalOperator::Projection>{
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    /// First inference catches the duplicate — the test here is about the round-trip path.
    /// But the query would normally fail before reaching serialization. Simulate a hypothetical
    /// round-trip by reflecting the raw (pre-inference) operator and then re-inferring.
    const auto reflected = NES::reflect(TypedLogicalOperator<ProjectionLogicalOperator>(op));
    const auto roundTripped = ReflectionContext{}.unreflect<TypedLogicalOperator<ProjectionLogicalOperator>>(reflected);

    EXPECT_THROW((void)roundTripped->withInferredSchema({schema}), Exception);
}

}
