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

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)
class InferModelLogicalOperatorTest : public ::testing::Test
{
protected:
    /// Register a fresh model in a shared test-local catalog and return the RegisteredModel.
    /// The catalog is a static local so entries accumulate across tests; names are made unique
    /// by a monotonic counter so re-registering is never needed.
    static RegisteredModel loadModel(std::string_view onnxFile, const Schema& inputs, const Schema& outputs)
    {
        static ModelCatalog catalog;
        static std::atomic<size_t> counter{0};
        const auto name = fmt::format("m_{}", counter.fetch_add(1));

        catalog.registerModel(
            name, std::filesystem::path(INFERENCE_TEST_DATA) / std::string(onnxFile), ModelSchema{.inputs = inputs, .outputs = outputs});
        return catalog.load(name);
    }

    /// Validate common output schema invariants: all model output fields are present and not nullable.
    static void validateOutputSchema(const Schema& outputSchema, const std::vector<std::string>& expectedOutputFields)
    {
        for (const auto& name : expectedOutputFields)
        {
            auto field = outputSchema.getFieldByName(name);
            ASSERT_TRUE(field.has_value()) << "Expected output field '" << name << "' not found in schema";
            EXPECT_FALSE(field->dataType.nullable) << "Output field '" << name << "' should not be nullable";
        }
    }

    /// Default operator: 1 FLOAT32 input "in_0", 1 FLOAT32 output "prediction" — backed by tiny_1_to_1.onnx.
    static InferModelLogicalOperator makeOp()
    {
        return InferModelLogicalOperator{
            loadModel(
                "tiny_1_to_1.onnx",
                Schema{}.addField("in_0", DataType::Type::FLOAT32),
                Schema{}.addField("prediction", DataType::Type::FLOAT32)),
            {"value"}};
    }
};

/// Basic properties: construction, wrapping, getName, explain, getters
TEST_F(InferModelLogicalOperatorTest, BasicProperties)
{
    auto op = makeOp();

    /// Can wrap in LogicalOperator{}
    EXPECT_NO_THROW({
        const LogicalOperator wrapped{op};
        (void)wrapped;
    });

    EXPECT_EQ(op.getName(), "InferModel");

    /// explain() short verbosity
    auto description = op.explain(ExplainVerbosity::Short, OperatorId{1});
    EXPECT_FALSE(description.empty());
    EXPECT_TRUE(description.find("INFER_MODEL") != std::string::npos);

    /// explain() debug verbosity includes opId and traitSet
    auto debugDesc = op.explain(ExplainVerbosity::Debug, OperatorId{42});
    EXPECT_TRUE(debugDesc.find("42") != std::string::npos);

    /// Getters
    EXPECT_EQ(op.getInputFieldNames(), std::vector<std::string>{"value"});
    EXPECT_EQ(op.getModel().getSchema().inputs.getNumberOfFields(), 1U);
    EXPECT_EQ(op.getModel().getSchema().outputs.getNumberOfFields(), 1U);
}

/// withChildren/getChildren and withTraitSet/getTraitSet — immutable copy semantics
TEST_F(InferModelLogicalOperatorTest, ImmutableCopySemantics)
{
    auto op = makeOp();

    /// Children round-trip
    EXPECT_TRUE(op.getChildren().empty());
    const InferModelLogicalOperator childOp{
        loadModel("tiny_1_to_1.onnx", Schema{}.addField("in", DataType::Type::FLOAT32), Schema{}.addField("out", DataType::Type::FLOAT32)),
        {"x"}};
    auto withChild = op.withChildren({LogicalOperator{childOp}});
    ASSERT_EQ(withChild.getChildren().size(), 1U);
    EXPECT_TRUE(op.getChildren().empty());

    /// TraitSet round-trip
    const TraitSet emptySet = op.getTraitSet();
    auto opWithTraits = op.withTraitSet(emptySet);
    EXPECT_EQ(opWithTraits.getTraitSet(), emptySet);

    /// Equality: two operators built against the same catalog entry and the same field names compare equal.
    /// NOLINTNEXTLINE(performance-unnecessary-copy-initialization) the copy is the point — tests operator== reflexivity across copies
    auto op2 = op;
    EXPECT_EQ(op, op2);
    const InferModelLogicalOperator op3{
        loadModel(
            "tiny_1_to_1.onnx",
            Schema{}.addField("in_0", DataType::Type::FLOAT32),
            Schema{}.addField("prediction", DataType::Type::FLOAT32)),
        {"other_field"}};
    EXPECT_NE(op, op3);
}

/// InferModelNameLogicalOperator construction, getName, explain, and wrapping
TEST_F(InferModelLogicalOperatorTest, NameVariantConstructionAndExplain)
{
    const InferModelNameLogicalOperator nameOp{"myModel", {"feature1", "feature2"}};

    EXPECT_EQ(std::string{nameOp.getName()}, "InferModelName");

    auto desc = nameOp.explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_FALSE(desc.empty());
    EXPECT_TRUE(desc.find("myModel") != std::string::npos);

    EXPECT_NO_THROW({
        const LogicalOperator wrapped{nameOp};
        (void)wrapped;
    });
}

/// Reflector/Unreflector round-trip: serialize and deserialize preserves operator state
TEST_F(InferModelLogicalOperatorTest, ReflectionRoundTrip)
{
    auto op = makeOp();

    /// Reflect
    const Reflector<InferModelLogicalOperator> reflector;
    auto reflected = reflector(op);

    /// Unreflect
    const Unreflector<InferModelLogicalOperator> unreflector;
    auto restored = unreflector(reflected);

    /// Verify preserved state
    EXPECT_EQ(restored.getInputFieldNames(), op.getInputFieldNames());
    EXPECT_EQ(restored.getModel(), op.getModel());
    EXPECT_EQ(restored.getName(), op.getName());

    const auto json = rfl::json::write(reflected);
    auto jsonReflected = rfl::json::read<Reflected>(json);
    ASSERT_TRUE(jsonReflected.has_value()) << jsonReflected.error().what();
    auto jsonRestored = unreflector(jsonReflected.value());
    EXPECT_EQ(jsonRestored.getModel(), op.getModel());
}

/// Schema inference: input schema propagates, output fields appended with correct types and ordering
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceHappyPath)
{
    /// Model: 1 FLOAT32 input, 2 FLOAT32 outputs (tiny_1_to_2.onnx has output shape [1,2])
    const InferModelLogicalOperator op{
        loadModel(
            "tiny_1_to_2.onnx",
            Schema{}.addField("in_0", DataType::Type::FLOAT32),
            Schema{}.addField("out_a", DataType::Type::FLOAT32).addField("out_b", DataType::Type::FLOAT32)),
        {"value"}};

    auto inputSchema = Schema{}.addField("value", DataType::Type::FLOAT32);
    auto inferred = op.withInferredSchema({inputSchema});

    /// Input schema preserved
    auto inputSchemas = inferred.getInputSchemas();
    ASSERT_EQ(inputSchemas.size(), 1U);
    EXPECT_EQ(inputSchemas[0], inputSchema);

    /// Output schema: input + model outputs, correct count, ordering, and types
    auto outputSchema = inferred.getOutputSchema();
    ASSERT_EQ(outputSchema.getNumberOfFields(), 3U);

    EXPECT_EQ(outputSchema.getFieldAt(0).getUnqualifiedName(), "value");
    EXPECT_EQ(outputSchema.getFieldAt(1).getUnqualifiedName(), "out_a");
    EXPECT_EQ(outputSchema.getFieldAt(1).dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));
    EXPECT_EQ(outputSchema.getFieldAt(2).getUnqualifiedName(), "out_b");
    EXPECT_EQ(outputSchema.getFieldAt(2).dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));

    validateOutputSchema(outputSchema, {"out_a", "out_b"});
}

/// Schema inference error cases: empty schemas, missing fields, type mismatches
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceErrors)
{
    /// Empty input schemas
    auto op = makeOp();
    ASSERT_EXCEPTION_ERRORCODE((void)op.withInferredSchema({}), NES::ErrorCode::CannotInferSchema);

    /// Field count mismatch: model expects 2, operator has 1
    const InferModelLogicalOperator op1{
        loadModel(
            "tiny_2_to_1.onnx",
            Schema{}.addField("a", DataType::Type::FLOAT32).addField("b", DataType::Type::FLOAT32),
            Schema{}.addField("result", DataType::Type::FLOAT32)),
        {"only_one_field"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op1.withInferredSchema({Schema{}.addField("only_one_field", DataType::Type::FLOAT32)}), NES::ErrorCode::CannotInferSchema);

    /// Missing field in schema
    const InferModelLogicalOperator op2{
        loadModel(
            "tiny_2_to_1.onnx",
            Schema{}.addField("a", DataType::Type::FLOAT32).addField("b", DataType::Type::FLOAT32),
            Schema{}.addField("result", DataType::Type::FLOAT32)),
        {"field_a", "field_b"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op2.withInferredSchema({Schema{}.addField("field_a", DataType::Type::FLOAT32)}), NES::ErrorCode::CannotInferSchema);

    /// Type mismatch: model expects Float32, schema has Int32
    const InferModelLogicalOperator op3{
        loadModel(
            "tiny_1_to_1.onnx",
            Schema{}.addField("in_0", DataType::Type::FLOAT32),
            Schema{}.addField("prediction", DataType::Type::FLOAT32)),
        {"value"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op3.withInferredSchema({Schema{}.addField("value", DataType::Type::INT32)}), NES::ErrorCode::CannotInferSchema);
}

/// VARSIZED input mirrors the whole tensor as bulk bytes; output can still be a scalar FLOAT32 field.
TEST_F(InferModelLogicalOperatorTest, VarsizedInputAccepted)
{
    const InferModelLogicalOperator op{
        loadModel(
            "tiny_1_to_1.onnx",
            Schema{}.addField("text", DataType::Type::VARSIZED),
            Schema{}.addField("embedding", DataType::Type::FLOAT32)),
        {"text"}};
    auto schema = Schema{}.addField("text", DataType::Type::VARSIZED);
    auto inferred = op.withInferredSchema({schema});
    validateOutputSchema(inferred.getOutputSchema(), {"embedding"});
}

/// VARSIZED both sides: passes bytes through verbatim.
TEST_F(InferModelLogicalOperatorTest, VarsizedOutputAccepted)
{
    const InferModelLogicalOperator op{
        loadModel(
            "tiny_1_to_1.onnx",
            Schema{}.addField("input_blob", DataType::Type::VARSIZED),
            Schema{}.addField("result_blob", DataType::Type::VARSIZED)),
        {"input_blob"}};
    auto schema = Schema{}.addField("input_blob", DataType::Type::VARSIZED);
    auto inferred = op.withInferredSchema({schema});

    auto outputSchema = inferred.getOutputSchema();
    auto resultField = outputSchema.getFieldByName("result_blob");
    ASSERT_TRUE(resultField.has_value());
    EXPECT_EQ(resultField->dataType, (DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}));
}

/// Nullable input fields are rejected; non-nullable multi-input is accepted
TEST_F(InferModelLogicalOperatorTest, NullableInputHandling)
{
    /// Single nullable field — rejected
    const InferModelLogicalOperator op1{
        loadModel(
            "tiny_1_to_1.onnx",
            Schema{}.addField("in_0", DataType::Type::FLOAT32),
            Schema{}.addField("prediction", DataType::Type::FLOAT32)),
        {"value"}};
    auto nullableSchema = Schema{}.addField("value", DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE);
    ASSERT_EXCEPTION_ERRORCODE((void)op1.withInferredSchema({nullableSchema}), NES::ErrorCode::CannotInferSchema);

    /// Multiple inputs, one nullable — rejected
    const InferModelLogicalOperator op2{
        loadModel(
            "tiny_2_to_1.onnx",
            Schema{}.addField("a", DataType::Type::FLOAT32).addField("b", DataType::Type::FLOAT32),
            Schema{}.addField("out", DataType::Type::FLOAT32)),
        {"a", "b"}};
    auto mixedSchema = Schema{}
                           .addField("a", DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE)
                           .addField("b", DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE);
    ASSERT_EXCEPTION_ERRORCODE((void)op2.withInferredSchema({mixedSchema}), NES::ErrorCode::CannotInferSchema);

    /// Multiple non-nullable inputs — accepted
    const InferModelLogicalOperator op3{
        loadModel(
            "tiny_2_to_1.onnx",
            Schema{}.addField("a", DataType::Type::FLOAT32).addField("b", DataType::Type::FLOAT32),
            Schema{}.addField("out", DataType::Type::FLOAT32)),
        {"a", "b"}};
    auto validSchema = Schema{}.addField("a", DataType::Type::FLOAT32).addField("b", DataType::Type::FLOAT32);
    auto inferred = op3.withInferredSchema({validSchema});
    validateOutputSchema(inferred.getOutputSchema(), {"out"});
}

/// Name collision between an upstream field and a model output field: the output schema
/// keeps a single entry with the model's declared type (here still FLOAT32, since the
/// catalog restricts model outputs to FLOAT32 or VARSIZED).
TEST_F(InferModelLogicalOperatorTest, NameCollisionReplacesFieldType)
{
    const InferModelLogicalOperator op{
        loadModel(
            "tiny_1_to_1.onnx", Schema{}.addField("in_0", DataType::Type::FLOAT32), Schema{}.addField("value", DataType::Type::FLOAT32)),
        {"input_field"}};

    auto inputSchema = Schema{}.addField("input_field", DataType::Type::FLOAT32).addField("value", DataType::Type::FLOAT32);
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    EXPECT_EQ(outputSchema.getNumberOfFields(), 2U);

    auto valueField = outputSchema.getFieldByName("value");
    ASSERT_TRUE(valueField.has_value());
    EXPECT_EQ(valueField->dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));
    validateOutputSchema(outputSchema, {"value"});
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
