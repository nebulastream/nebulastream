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
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <BaseUnitTest.hpp>
#include <ConditionalLogicalFunction.hpp>
#include <ErrorHandling.hpp>

using namespace NES;

/// A minimal conditional: one condition, one result, one default.
TEST(ConditionalLogicalFunctionTest, InferDataTypeSimpleTwoWay)
{
    auto schema = Schema{}
                      .addField("stream$a", DataType{DataType::Type::INT32})
                      .addField("stream$b", DataType{DataType::Type::INT32})
                      .addField("stream$flag", DataType{DataType::Type::BOOLEAN});

    /// Conditional(flag, a, b) — picks between two fields
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        FieldAccessLogicalFunction("a"),
        FieldAccessLogicalFunction("b"),
    }));
    auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::INT32));
}

/// Multi-branch conditional with three conditions.
TEST(ConditionalLogicalFunctionTest, InferDataTypeMultiBranch)
{
    auto schema
        = Schema{}.addField("stream$id", DataType{DataType::Type::INT32}).addField("stream$flag", DataType{DataType::Type::BOOLEAN});

    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "A"),
        GreaterLogicalFunction(FieldAccessLogicalFunction("id"), ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "5")),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "B"),
        LessLogicalFunction(FieldAccessLogicalFunction("id"), ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0")),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "C"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "D"),
    }));
    auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::VARSIZED));
}

/// Type inference infers the result type from the default (last child).
TEST(ConditionalLogicalFunctionTest, InferDataTypeFromDefault)
{
    auto schema = Schema{}
                      .addField("stream$temperature", DataType{DataType::Type::FLOAT32})
                      .addField("stream$flag", DataType{DataType::Type::BOOLEAN});

    /// Conditional(flag, temperature, 0.0) — mixes field access and constant
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        FieldAccessLogicalFunction("temperature"),
        ConstantValueLogicalFunction(DataType{DataType::Type::FLOAT32}, "0.0"),
    }));
    auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::FLOAT32));
}

/// Condition that is not BOOLEAN should fail validation (PRECONDITION terminates).
TEST(ConditionalLogicalFunctionTest, RejectsNonBooleanCondition)
{
    auto schema = Schema{}.addField("stream$id", DataType{DataType::Type::INT32});

    /// Conditional(id, "yes", "no") — id is INT32, not BOOLEAN
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("id"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "yes"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "no"),
    }));
    ASSERT_DEATH_DEBUG(static_cast<void>(fn.withInferredDataType(schema)), "");
}

/// Non-BOOLEAN condition at a later index (not the first) should still fail validation.
TEST(ConditionalLogicalFunctionTest, RejectsNonBooleanSecondCondition)
{
    auto schema
        = Schema{}.addField("stream$flag", DataType{DataType::Type::BOOLEAN}).addField("stream$id", DataType{DataType::Type::INT32});

    /// First condition is BOOLEAN, second "condition" is INT32
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "A"),
        FieldAccessLogicalFunction("id"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "B"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "C"),
    }));
    ASSERT_DEATH_DEBUG(static_cast<void>(fn.withInferredDataType(schema)), "");
}

/// Result types that don't match the default should fail validation.
TEST(ConditionalLogicalFunctionTest, RejectsMismatchedResultTypes)
{
    auto schema
        = Schema{}.addField("stream$id", DataType{DataType::Type::INT32}).addField("stream$flag", DataType{DataType::Type::BOOLEAN});

    /// Conditional(flag, id, "hello") — INT32 field vs VARSIZED default
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        FieldAccessLogicalFunction("id"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "hello"),
    }));
    ASSERT_DEATH_DEBUG(static_cast<void>(fn.withInferredDataType(schema)), "");
}

/// Deserialization rejects even number of children (missing default).
TEST(ConditionalLogicalFunctionTest, RejectsEvenNumberOfChildren)
{
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        ConstantValueLogicalFunction(DataType{DataType::Type::BOOLEAN}, "true"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "yes"),
    }));
    auto reflected = reflect(fn);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(unreflect<LogicalFunction>(reflected)), ErrorCode::CannotDeserialize);
}

/// Deserialization rejects fewer than 3 children.
TEST(ConditionalLogicalFunctionTest, RejectsTooFewChildren)
{
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "x"),
    }));
    auto reflected = reflect(fn);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(unreflect<LogicalFunction>(reflected)), ErrorCode::CannotDeserialize);
}

/// Explain produces a readable CASE WHEN string with correct structure.
TEST(ConditionalLogicalFunctionTest, Explain)
{
    auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("cond1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "r1"),
        FieldAccessLogicalFunction("cond2"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "r2"),
        FieldAccessLogicalFunction("cond3"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "r3"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "fallback"),
    }));
    auto explanation = fn.explain(ExplainVerbosity::Short);
    EXPECT_EQ(explanation, "CASE WHEN cond1 THEN r1 WHEN cond2 THEN r2 WHEN cond3 THEN r3 ELSE fallback");
}

/// Identical functions should compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityIdentical)
{
    auto a = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    auto b = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    EXPECT_TRUE(a == b);
}

/// Functions with different children should not compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityDifferent)
{
    auto a = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    auto b = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "2"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    EXPECT_FALSE(a == b);
}

/// Serialize then deserialize should produce an equal function.
TEST(ConditionalLogicalFunctionTest, SerializeDeserializeRoundtrip)
{
    auto original = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "A"),
        GreaterLogicalFunction(FieldAccessLogicalFunction("id"), ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "5")),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "B"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED}, "C"),
    }));

    auto reflected = reflect(original);
    auto deserialized = unreflect<LogicalFunction>(reflected);
    EXPECT_TRUE(original == deserialized);
}

/// Serialize/deserialize roundtrip for a simple two-way conditional.
TEST(ConditionalLogicalFunctionTest, SerializeDeserializeSimple)
{
    auto original = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    }));

    auto reflected = reflect(original);
    auto deserialized = unreflect<LogicalFunction>(reflected);
    EXPECT_TRUE(original == deserialized);
}

/// getChildren returns all children in order.
TEST(ConditionalLogicalFunctionTest, GetChildrenReturnsAll)
{
    auto fn = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    EXPECT_EQ(fn.getChildren().size(), 3u);
}

/// getType returns "Conditional".
TEST(ConditionalLogicalFunctionTest, GetType)
{
    auto fn = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    EXPECT_EQ(fn.getType(), "Conditional");
}

/// getDataType returns the type of the default (last) child.
TEST(ConditionalLogicalFunctionTest, GetDataTypeMatchesDefault)
{
    auto fn = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32}, "0"),
    });
    EXPECT_TRUE(fn.getDataType().isType(DataType::Type::INT32));
}
