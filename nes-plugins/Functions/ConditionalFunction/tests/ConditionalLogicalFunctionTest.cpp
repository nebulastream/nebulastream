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
    const auto schema = Schema{}
                      .addField("stream$a", DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE})
                      .addField("stream$b", DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE})
                      .addField("stream$flag", DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE});

    /// Conditional(flag, a, b) — picks between two fields
    const auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        FieldAccessLogicalFunction("a"),
        FieldAccessLogicalFunction("b"),
    }));
    const auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::INT32));
}

/// Multi-branch conditional with three conditions.
TEST(ConditionalLogicalFunctionTest, InferDataTypeMultiBranch)
{
    const auto schema
        = Schema{}.addField("stream$id", DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}).addField("stream$flag", DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE});

    const auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "A"),
        GreaterLogicalFunction(FieldAccessLogicalFunction("id"), ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "5")),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "B"),
        LessLogicalFunction(FieldAccessLogicalFunction("id"), ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0")),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "C"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "D"),
    }));
    const auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::VARSIZED));
}

/// Type inference infers the result type from the default (last child).
TEST(ConditionalLogicalFunctionTest, InferDataTypeFromDefault)
{
    const auto schema = Schema{}
                      .addField("stream$temperature", DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE})
                      .addField("stream$flag", DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE});

    /// Conditional(flag, temperature, 0.0) — mixes field access and constant
    const auto fn = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        FieldAccessLogicalFunction("temperature"),
        ConstantValueLogicalFunction(DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}, "0.0"),
    }));
    auto inferred = fn.withInferredDataType(schema);
    EXPECT_TRUE(inferred.getDataType().isType(DataType::Type::FLOAT32));
}

/// Deserialization rejects even number of children (missing default).
TEST(ConditionalLogicalFunctionTest, RejectsEvenNumberOfChildren)
{
    const auto fn = LogicalFunction(ConditionalLogicalFunction({
        ConstantValueLogicalFunction(DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE}, "true"),
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "yes"),
    }));
    const auto reflected = reflect(fn);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(ReflectionContext{}.unreflect<LogicalFunction>(reflected)), ErrorCode::CannotDeserialize);
}

/// Deserialization rejects fewer than 3 children.
TEST(ConditionalLogicalFunctionTest, RejectsTooFewChildren)
{
    const auto fn = LogicalFunction(ConditionalLogicalFunction({
        ConstantValueLogicalFunction(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "x"),
    }));
    auto reflected = reflect(fn);
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(ReflectionContext{}.unreflect<LogicalFunction>(reflected)), ErrorCode::CannotDeserialize);
}

/// Identical functions should compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityIdentical)
{
    const auto a = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    const auto b = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    EXPECT_TRUE(a == b);
}

/// Functions with different children should not compare equal.
TEST(ConditionalLogicalFunctionTest, EqualityDifferent)
{
    const auto a = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    const auto b = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "2"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    EXPECT_FALSE(a == b);
}

/// Serialize/deserialize round-trip for a simple two-way conditional.
TEST(ConditionalLogicalFunctionTest, SerializeDeserializeSimple)
{
    const auto original = LogicalFunction(ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    }));

    const auto reflected = reflect(original);
    const auto deserialized = ReflectionContext{}.unreflect<LogicalFunction>(reflected);
    EXPECT_TRUE(original == deserialized);
}

/// getType returns "Conditional".
TEST(ConditionalLogicalFunctionTest, GetType)
{
    const auto fn = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    EXPECT_EQ(fn.getType(), "Conditional");
}

/// getDataType returns the type of the default (last) child.
TEST(ConditionalLogicalFunctionTest, GetDataTypeMatchesDefault)
{
    const auto fn = ConditionalLogicalFunction({
        FieldAccessLogicalFunction("flag"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "1"),
        ConstantValueLogicalFunction(DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}, "0"),
    });
    EXPECT_TRUE(fn.getDataType().isType(DataType::Type::INT32));
}
