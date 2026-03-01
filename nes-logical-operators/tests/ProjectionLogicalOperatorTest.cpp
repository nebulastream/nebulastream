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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Wraps withInferredSchema to discard the [[nodiscard]] result in no-throw tests.
void inferSchema(const ProjectionLogicalOperator& op, std::vector<Schema> schemas)
{
    (void)op.withInferredSchema(std::move(schemas));
}
}

class ProjectionLogicalOperatorTest : public ::testing::Test
{
protected:
    Schema schema = Schema{}.addField("stream.a", DataType::Type::UINT64).addField("stream.b", DataType::Type::UINT64);
};

/// Two projections with the same explicit alias must be rejected with CannotInferSchema (code 2003).
TEST_F(ProjectionLogicalOperatorTest, DuplicateExplicitAliasThrows)
{
    const ProjectionLogicalOperator op(
        {
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_THROW(inferSchema(op, {schema}), Exception);
    try
    {
        inferSchema(op, {schema});
        FAIL() << "Expected Exception with CannotInferSchema code";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::CannotInferSchema);
    }
}

/// Two unaliased projections accessing the same field must be rejected (ambiguous output schema).
TEST_F(ProjectionLogicalOperatorTest, DuplicateUnaliasedFieldThrows)
{
    const ProjectionLogicalOperator op(
        {
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_THROW(inferSchema(op, {schema}), Exception);
    try
    {
        inferSchema(op, {schema});
        FAIL() << "Expected Exception with CannotInferSchema code";
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::CannotInferSchema);
    }
}

/// An alias that collides with an unaliased field must be rejected (e.g. SELECT id, 5 AS id).
TEST_F(ProjectionLogicalOperatorTest, AliasCollidingWithUnaliasedFieldThrows)
{
    const ProjectionLogicalOperator op(
        {
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("stream.a"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_THROW(inferSchema(op, {schema}), Exception);
}

/// Projections with distinct aliases must succeed.
TEST_F(ProjectionLogicalOperatorTest, UniqueAliasesSucceeds)
{
    const ProjectionLogicalOperator op(
        {
            {FieldIdentifier("x"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
            {FieldIdentifier("y"), LogicalFunction(FieldAccessLogicalFunction("stream.b"))},
        },
        ProjectionLogicalOperator::Asterisk(false));

    EXPECT_NO_THROW(inferSchema(op, {schema}));
}

/// Asterisk plus an unaliased projection that duplicates an input field name must be rejected.
TEST_F(ProjectionLogicalOperatorTest, AsteriskPlusDuplicateProjectionThrows)
{
    /// The asterisk contributes "stream.a" and "stream.b".
    /// An unaliased projection for "stream.a" duplicates the asterisk field.
    const ProjectionLogicalOperator op(
        {
            {std::nullopt, LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(true));

    EXPECT_THROW(inferSchema(op, {schema}), Exception);
}

/// Asterisk plus a distinctly aliased projection must succeed.
TEST_F(ProjectionLogicalOperatorTest, AsteriskPlusNewAliasSucceeds)
{
    const ProjectionLogicalOperator op(
        {
            {FieldIdentifier("new_a"), LogicalFunction(FieldAccessLogicalFunction("stream.a"))},
        },
        ProjectionLogicalOperator::Asterisk(true));

    EXPECT_NO_THROW(inferSchema(op, {schema}));
}

}
