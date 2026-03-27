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

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)
class InferModelLogicalOperatorTest : public ::testing::Test
{
protected:
    /// Build a test Model from input type list and named output list.
    /// No bytecode is needed — schema inference only reads getInputs() / getOutputs().
    static NES::Model
    makeModel(const std::vector<DataType::Type>& inputs, const std::vector<std::pair<std::string, DataType::Type>>& outputs)
    {
        Model model;
        std::vector<DataType> inputTypes;
        inputTypes.reserve(inputs.size());
        for (auto inputType : inputs)
        {
            inputTypes.emplace_back(inputType, DataType::NULLABLE::NOT_NULLABLE);
        }
        model.setInputs(std::move(inputTypes));

        std::vector<std::pair<std::string, DataType>> outputFields;
        outputFields.reserve(outputs.size());
        for (const auto& [name, type] : outputs)
        {
            outputFields.emplace_back(name, DataType{type, DataType::NULLABLE::NOT_NULLABLE});
        }
        model.setOutputs(std::move(outputFields));
        return model;
    }

    /// Build a Schema from a list of (name, DataType::Type) pairs.
    static Schema makeSchema(const std::vector<std::pair<std::string, DataType::Type>>& fields)
    {
        Schema schema;
        for (const auto& [name, type] : fields)
        {
            schema = schema.addField(name, DataType{type, DataType::NULLABLE::NOT_NULLABLE});
        }
        return schema;
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

    /// Default operator: 1 Float32 input "value", 1 Float32 output "prediction"
    static InferModelLogicalOperator makeOp()
    {
        auto model = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
        return InferModelLogicalOperator{std::move(model), {"value"}};
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
    EXPECT_EQ(op.getModel().getInputs().size(), 1U);
    EXPECT_EQ(op.getModel().getOutputs().size(), 1U);
}

/// withChildren/getChildren and withTraitSet/getTraitSet — immutable copy semantics
TEST_F(InferModelLogicalOperatorTest, ImmutableCopySemantics)
{
    auto op = makeOp();

    /// Children round-trip
    EXPECT_TRUE(op.getChildren().empty());
    auto childModel = makeModel({DataType::Type::FLOAT32}, {{"out", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator childOp{std::move(childModel), {"x"}};
    auto withChild = op.withChildren({LogicalOperator{childOp}});
    ASSERT_EQ(withChild.getChildren().size(), 1U);
    EXPECT_TRUE(op.getChildren().empty());

    /// TraitSet round-trip
    const TraitSet emptySet = op.getTraitSet();
    auto opWithTraits = op.withTraitSet(emptySet);
    EXPECT_EQ(opWithTraits.getTraitSet(), emptySet);

    /// Equality
    auto op2 = makeOp();
    EXPECT_EQ(op, op2);
    auto model3 = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op3{std::move(model3), {"other_field"}};
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
    EXPECT_EQ(restored.getModel().getInputs(), op.getModel().getInputs());
    EXPECT_EQ(restored.getModel().getOutputs(), op.getModel().getOutputs());
    EXPECT_EQ(restored.getName(), op.getName());
}

/// Schema inference: input schema propagates, output fields appended with correct types and ordering
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceHappyPath)
{
    /// Model: 1 Float32 input, 2 outputs with mixed types
    auto model = makeModel({DataType::Type::FLOAT32}, {{"out_a", DataType::Type::FLOAT32}, {"out_b", DataType::Type::INT32}});
    const InferModelLogicalOperator op{std::move(model), {"value"}};

    auto inputSchema = makeSchema({{"value", DataType::Type::FLOAT32}});
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
    EXPECT_EQ(outputSchema.getFieldAt(2).dataType, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}));

    validateOutputSchema(outputSchema, {"out_a", "out_b"});
}

/// Schema inference error cases: empty schemas, missing fields, type mismatches
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceErrors)
{
    /// Empty input schemas
    auto op = makeOp();
    ASSERT_EXCEPTION_ERRORCODE((void)op.withInferredSchema({}), NES::ErrorCode::CannotInferSchema);

    /// Field count mismatch: model expects 2, operator has 1
    auto model1 = makeModel({DataType::Type::FLOAT32, DataType::Type::FLOAT32}, {{"result", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op1{std::move(model1), {"only_one_field"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op1.withInferredSchema({makeSchema({{"only_one_field", DataType::Type::FLOAT32}})}), NES::ErrorCode::CannotInferSchema);

    /// Missing field in schema
    auto model2 = makeModel({DataType::Type::FLOAT32, DataType::Type::FLOAT32}, {{"result", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op2{std::move(model2), {"field_a", "field_b"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op2.withInferredSchema({makeSchema({{"field_a", DataType::Type::FLOAT32}})}), NES::ErrorCode::CannotInferSchema);

    /// Type mismatch: model expects Float32, schema has Int32
    auto model3 = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op3{std::move(model3), {"value"}};
    ASSERT_EXCEPTION_ERRORCODE(
        (void)op3.withInferredSchema({makeSchema({{"value", DataType::Type::INT32}})}), NES::ErrorCode::CannotInferSchema);
}

/// VARSIZED type is accepted when model expects it
TEST_F(InferModelLogicalOperatorTest, VarsizedInputAccepted)
{
    auto varsizedModel = makeModel({DataType::Type::VARSIZED}, {{"embedding", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op{std::move(varsizedModel), {"text"}};
    auto schema = makeSchema({{"text", DataType::Type::VARSIZED}});
    auto inferred = op.withInferredSchema({schema});
    validateOutputSchema(inferred.getOutputSchema(), {"embedding"});
}

/// VARSIZED output type is accepted when model produces it
TEST_F(InferModelLogicalOperatorTest, VarsizedOutputAccepted)
{
    auto varsizedOutputModel = makeModel({DataType::Type::VARSIZED}, {{"result_blob", DataType::Type::VARSIZED}});
    const InferModelLogicalOperator op{std::move(varsizedOutputModel), {"input_blob"}};
    auto schema = makeSchema({{"input_blob", DataType::Type::VARSIZED}});
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
    auto model1 = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op1{std::move(model1), {"value"}};
    Schema nullableSchema;
    nullableSchema = nullableSchema.addField("value", DataType{DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE});
    ASSERT_EXCEPTION_ERRORCODE((void)op1.withInferredSchema({nullableSchema}), NES::ErrorCode::CannotInferSchema);

    /// Multiple inputs, one nullable — rejected
    auto model2 = makeModel({DataType::Type::FLOAT32, DataType::Type::FLOAT32}, {{"out", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op2{std::move(model2), {"a", "b"}};
    Schema mixedSchema;
    mixedSchema = mixedSchema.addField("a", DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE});
    mixedSchema = mixedSchema.addField("b", DataType{DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE});
    ASSERT_EXCEPTION_ERRORCODE((void)op2.withInferredSchema({mixedSchema}), NES::ErrorCode::CannotInferSchema);

    /// Multiple non-nullable inputs with mixed types — accepted
    auto model3 = makeModel({DataType::Type::FLOAT32, DataType::Type::INT32}, {{"out", DataType::Type::FLOAT32}});
    const InferModelLogicalOperator op3{std::move(model3), {"a", "b"}};
    auto validSchema = makeSchema({{"a", DataType::Type::FLOAT32}, {"b", DataType::Type::INT32}});
    auto inferred = op3.withInferredSchema({validSchema});
    validateOutputSchema(inferred.getOutputSchema(), {"out"});
}

/// Name collision replaces existing field type rather than duplicating
TEST_F(InferModelLogicalOperatorTest, NameCollisionReplacesFieldType)
{
    auto model = makeModel({DataType::Type::FLOAT32}, {{"value", DataType::Type::INT32}});
    const InferModelLogicalOperator op{std::move(model), {"input_field"}};

    auto inputSchema = makeSchema({{"input_field", DataType::Type::FLOAT32}, {"value", DataType::Type::FLOAT32}});
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    EXPECT_EQ(outputSchema.getNumberOfFields(), 2U);

    auto valueField = outputSchema.getFieldByName("value");
    ASSERT_TRUE(valueField.has_value());
    EXPECT_EQ(valueField->dataType, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}));
    validateOutputSchema(outputSchema, {"value"});
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
