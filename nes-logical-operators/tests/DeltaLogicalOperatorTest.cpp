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

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/DeltaLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

using namespace NES;

class DeltaLogicalOperatorTest : public ::testing::Test
{
protected:
    Schema inputSchema = Schema{}
                             .addField("stream$id", DataType{DataType::Type::UINT64})
                             .addField("stream$value", DataType{DataType::Type::UINT64})
                             .addField("stream$timestamp", DataType{DataType::Type::UINT64});
};

/// Schema inference with a single aliased delta expression should pass through all input fields and append the delta field.
TEST_F(DeltaLogicalOperatorTest, InferSchemaWithAlias)
{
    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
    });

    auto inferred = deltaOp.withInferredSchema({inputSchema});
    auto outputSchema = inferred.getOutputSchema();

    EXPECT_EQ(outputSchema.getNumberOfFields(), 4u);
    EXPECT_TRUE(outputSchema.contains("stream$id"));
    EXPECT_TRUE(outputSchema.contains("stream$value"));
    EXPECT_TRUE(outputSchema.contains("stream$timestamp"));
    EXPECT_TRUE(outputSchema.contains("delta_value"));

    /// Original fields keep their types
    EXPECT_TRUE(outputSchema.getFieldByName("stream$id")->dataType.isType(DataType::Type::UINT64));
    EXPECT_TRUE(outputSchema.getFieldByName("stream$value")->dataType.isType(DataType::Type::UINT64));
    EXPECT_TRUE(outputSchema.getFieldByName("stream$timestamp")->dataType.isType(DataType::Type::UINT64));

    /// Delta field should be signed
    EXPECT_TRUE(outputSchema.getFieldByName("delta_value")->dataType.isType(DataType::Type::INT64));
}

/// Schema inference with multiple delta expressions should append all aliased delta fields.
TEST_F(DeltaLogicalOperatorTest, InferSchemaMultipleDeltaExpressions)
{
    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_id"), FieldAccessLogicalFunction("id")},
        {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
    });

    auto inferred = deltaOp.withInferredSchema({inputSchema});
    auto outputSchema = inferred.getOutputSchema();

    EXPECT_EQ(outputSchema.getNumberOfFields(), 5u);
    EXPECT_TRUE(outputSchema.contains("delta_id"));
    EXPECT_TRUE(outputSchema.contains("delta_value"));
    EXPECT_TRUE(outputSchema.getFieldByName("delta_id")->dataType.isType(DataType::Type::INT64));
    EXPECT_TRUE(outputSchema.getFieldByName("delta_value")->dataType.isType(DataType::Type::INT64));

    /// Original fields unchanged
    EXPECT_TRUE(outputSchema.getFieldByName("stream$id")->dataType.isType(DataType::Type::UINT64));
    EXPECT_TRUE(outputSchema.getFieldByName("stream$value")->dataType.isType(DataType::Type::UINT64));
}

/// Delta on float fields should preserve the float type since floats are already signed.
TEST_F(DeltaLogicalOperatorTest, InferSchemaFloatField)
{
    auto floatSchema = Schema{}
                           .addField("stream$temperature", DataType{DataType::Type::FLOAT64})
                           .addField("stream$timestamp", DataType{DataType::Type::UINT64});

    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_temp"), FieldAccessLogicalFunction("temperature")},
    });

    auto inferred = deltaOp.withInferredSchema({floatSchema});
    auto outputSchema = inferred.getOutputSchema();

    EXPECT_EQ(outputSchema.getNumberOfFields(), 3u);
    EXPECT_TRUE(outputSchema.getFieldByName("delta_temp")->dataType.isType(DataType::Type::FLOAT64));
}

/// Delta on a field with a smaller integer type should produce the corresponding signed type.
TEST_F(DeltaLogicalOperatorTest, InferSchemaSmallIntegerTypes)
{
    auto smallSchema = Schema{}
                           .addField("stream$a", DataType{DataType::Type::UINT8})
                           .addField("stream$b", DataType{DataType::Type::UINT16})
                           .addField("stream$c", DataType{DataType::Type::UINT32})
                           .addField("stream$d", DataType{DataType::Type::INT32});

    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_a"), FieldAccessLogicalFunction("a")},
        {FieldIdentifier("delta_b"), FieldAccessLogicalFunction("b")},
        {FieldIdentifier("delta_c"), FieldAccessLogicalFunction("c")},
        {FieldIdentifier("delta_d"), FieldAccessLogicalFunction("d")},
    });

    auto inferred = deltaOp.withInferredSchema({smallSchema});
    auto outputSchema = inferred.getOutputSchema();

    EXPECT_TRUE(outputSchema.getFieldByName("delta_a")->dataType.isType(DataType::Type::INT8));
    EXPECT_TRUE(outputSchema.getFieldByName("delta_b")->dataType.isType(DataType::Type::INT16));
    EXPECT_TRUE(outputSchema.getFieldByName("delta_c")->dataType.isType(DataType::Type::INT32));
    EXPECT_TRUE(outputSchema.getFieldByName("delta_d")->dataType.isType(DataType::Type::INT32));
}

/// Delta should reject non-numeric field types.
TEST_F(DeltaLogicalOperatorTest, InferSchemaRejectsNonNumericType)
{
    auto boolSchema = Schema{}.addField("stream$flag", DataType{DataType::Type::BOOLEAN});

    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_flag"), FieldAccessLogicalFunction("flag")},
    });

    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(deltaOp.withInferredSchema({boolSchema})), ErrorCode::CannotInferSchema);
}

/// Delta should reject empty input schemas.
TEST_F(DeltaLogicalOperatorTest, InferSchemaRejectsEmptyInput)
{
    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
    });

    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(deltaOp.withInferredSchema({})), ErrorCode::CannotInferSchema);
}

/// Delta satisfies the LogicalOperatorConcept and can be wrapped in a LogicalOperator.
TEST_F(DeltaLogicalOperatorTest, WrapsInLogicalOperator)
{
    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
    });

    LogicalOperator op{deltaOp};
    EXPECT_EQ(op.getName(), "Delta");

    auto retrieved = op.tryGetAs<DeltaLogicalOperator>();
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->get().getDeltaExpressions().size(), 1u);
}

/// Explain should produce a readable string for debugging.
TEST_F(DeltaLogicalOperatorTest, Explain)
{
    auto deltaOp = DeltaLogicalOperator({
        {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
    });

    auto inferred = deltaOp.withInferredSchema({inputSchema});
    LogicalOperator op{inferred};

    auto explanation = op.explain(ExplainVerbosity::Short);
    EXPECT_NE(explanation.find("DELTA"), std::string::npos);
    EXPECT_NE(explanation.find("value"), std::string::npos);
    EXPECT_NE(explanation.find("delta_value"), std::string::npos);
}

/// Equal operators constructed the same way should compare equal.
TEST_F(DeltaLogicalOperatorTest, EqualityIdenticalOperators)
{
    auto a = DeltaLogicalOperator({{FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")}});
    auto b = DeltaLogicalOperator({{FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")}});

    auto inferredA = a.withInferredSchema({inputSchema});
    auto inferredB = b.withInferredSchema({inputSchema});

    EXPECT_TRUE(inferredA == inferredB);
}

/// Operators with different delta expressions should not compare equal.
TEST_F(DeltaLogicalOperatorTest, EqualityDifferentExpressions)
{
    auto a
        = DeltaLogicalOperator({{FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")}}).withInferredSchema({inputSchema});
    auto b = DeltaLogicalOperator({{FieldIdentifier("delta_id"), FieldAccessLogicalFunction("id")}}).withInferredSchema({inputSchema});

    EXPECT_FALSE(a == b);
}

/// Operators with different schemas should not compare equal.
TEST_F(DeltaLogicalOperatorTest, EqualityDifferentSchemas)
{
    auto otherSchema = Schema{}.addField("stream$x", DataType{DataType::Type::UINT64});

    auto a
        = DeltaLogicalOperator({{FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")}}).withInferredSchema({inputSchema});
    auto b = DeltaLogicalOperator({{FieldIdentifier("delta_x"), FieldAccessLogicalFunction("x")}}).withInferredSchema({otherSchema});

    EXPECT_FALSE(a == b);
}

/// Serialize then deserialize should produce an equal operator.
TEST_F(DeltaLogicalOperatorTest, SerializeDeserializeRoundtrip)
{
    auto cases = std::vector<DeltaLogicalOperator>{
        /// Single aliased expression
        DeltaLogicalOperator({{FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")}}),
        /// Multiple aliased expressions
        DeltaLogicalOperator({
            {FieldIdentifier("delta_id"), FieldAccessLogicalFunction("id")},
            {FieldIdentifier("delta_value"), FieldAccessLogicalFunction("value")},
        }),
    };

    for (auto& deltaOp : cases)
    {
        auto original = LogicalOperator{deltaOp.withInferredSchema({inputSchema})};

        auto reflected = OperatorSerializationUtil::serializeOperator(original);
        auto deserialized = OperatorSerializationUtil::deserializeOperator(reflected);

        ASSERT_TRUE(deserialized.tryGetAs<DeltaLogicalOperator>().has_value());
        EXPECT_TRUE(original == deserialized);
    }
}
