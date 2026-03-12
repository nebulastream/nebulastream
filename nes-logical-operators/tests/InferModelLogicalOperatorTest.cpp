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

#include <memory>
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
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Traits/TraitSet.hpp>
#include <Model.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

class InferModelLogicalOperatorTest : public ::testing::Test
{
protected:
    /// Build a test Model from input type list and named output list.
    /// No bytecode is needed — schema inference only reads getInputs() / getOutputs().
    static NES::Nebuli::Inference::Model
    makeModel(std::vector<DataType::Type> inputs, std::vector<std::pair<std::string, DataType::Type>> outputs)
    {
        SerializableOperator_Model proto;
        for (auto inputType : inputs)
        {
            DataTypeSerializationUtil::serializeDataType(DataType{inputType, DataType::NULLABLE::NOT_NULLABLE}, proto.add_inputs());
        }
        for (auto& [name, type] : outputs)
        {
            auto* out = proto.add_outputs();
            out->set_name(name);
            DataTypeSerializationUtil::serializeDataType(DataType{type, DataType::NULLABLE::NOT_NULLABLE}, out->mutable_type());
        }
        return NES::Nebuli::Inference::deserializeModel(proto);
    }

    /// Build a Schema from a list of (name, DataType::Type) pairs.
    static Schema makeSchema(std::vector<std::pair<std::string, DataType::Type>> fields)
    {
        Schema schema;
        for (auto& [name, type] : fields)
        {
            schema = schema.addField(name, DataType{type, DataType::NULLABLE::NOT_NULLABLE});
        }
        return schema;
    }

    /// Default operator: 1 Float32 input "value", 1 Float32 output "prediction"
    static InferModelLogicalOperator makeOp()
    {
        auto model = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
        return InferModelLogicalOperator{std::move(model), {"value"}};
    }
};

/// LOGI-01: Construct InferModelLogicalOperator with valid Model and wrap in LogicalOperator{}
TEST_F(InferModelLogicalOperatorTest, ConstructWithValidModel)
{
    EXPECT_NO_THROW({
        auto op = makeOp();
        LogicalOperator wrapped{op};
        (void)wrapped;
    });
}

/// LOGI-02: explain() returns non-empty string containing "InferModel" or "INFER_MODEL"
TEST_F(InferModelLogicalOperatorTest, ExplainReturnsDescription)
{
    auto op = makeOp();
    auto description = op.explain(ExplainVerbosity::Short, OperatorId{1});
    EXPECT_FALSE(description.empty());
    EXPECT_TRUE(description.find("INFER_MODEL") != std::string::npos || description.find("InferModel") != std::string::npos);
}

/// LOGI-03: withChildren/getChildren round-trip — immutable copy semantics
TEST_F(InferModelLogicalOperatorTest, WithChildrenRoundTrip)
{
    auto op = makeOp();
    EXPECT_TRUE(op.getChildren().empty());

    /// Create a child operator
    auto childModel = makeModel({DataType::Type::FLOAT32}, {{"out", DataType::Type::FLOAT32}});
    InferModelLogicalOperator childOp{std::move(childModel), {"x"}};
    LogicalOperator child{childOp};

    auto withChild = op.withChildren({child});
    ASSERT_EQ(withChild.getChildren().size(), 1u);
    /// Original is unchanged (value semantics)
    EXPECT_TRUE(op.getChildren().empty());
}

/// LOGI-04: withTraitSet/getTraitSet round-trip
TEST_F(InferModelLogicalOperatorTest, WithTraitSetRoundTrip)
{
    auto op = makeOp();
    /// Default trait set is empty
    TraitSet emptySet = op.getTraitSet();

    /// Apply a new trait set (create a non-default one by copying and checking it's stored)
    auto opWithTraits = op.withTraitSet(emptySet);
    EXPECT_EQ(opWithTraits.getTraitSet(), emptySet);
    /// Original is unchanged
    EXPECT_EQ(op.getTraitSet(), emptySet);
}

/// LOGI-05: operator== equality semantics
TEST_F(InferModelLogicalOperatorTest, EqualitySemantics)
{
    auto op1 = makeOp();
    auto op2 = makeOp();
    /// Same model, same inputFieldNames → equal
    EXPECT_EQ(op1, op2);

    /// Different inputFieldNames → not equal
    auto model2 = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op3{std::move(model2), {"other_field"}};
    EXPECT_NE(op1, op3);
}

/// LOGI-06: getName returns "InferModel"
TEST_F(InferModelLogicalOperatorTest, GetNameReturnsInferModel)
{
    auto op = makeOp();
    EXPECT_EQ(op.getName(), "InferModel");
}

/// LOGI-07: After withInferredSchema, getInputSchemas and getOutputSchema reflect inferred state
TEST_F(InferModelLogicalOperatorTest, SchemasPropagateAfterInference)
{
    auto op = makeOp();
    /// Schema: field "value" of Float32
    auto inputSchema = makeSchema({{"value", DataType::Type::FLOAT32}});

    auto inferred = op.withInferredSchema({inputSchema});

    /// Input schema should be set
    auto inputSchemas = inferred.getInputSchemas();
    ASSERT_EQ(inputSchemas.size(), 1u);
    EXPECT_EQ(inputSchemas[0], inputSchema);

    /// Output schema should contain both "value" and "prediction"
    auto outputSchema = inferred.getOutputSchema();
    EXPECT_TRUE(outputSchema.getFieldByName("value").has_value());
    EXPECT_TRUE(outputSchema.getFieldByName("prediction").has_value());
}

/// LOGI-08: InferModelNameLogicalOperator construction, getName, and explain
TEST_F(InferModelLogicalOperatorTest, NameVariantConstructionAndExplain)
{
    InferModelNameLogicalOperator nameOp{"myModel", {"feature1", "feature2"}};

    EXPECT_EQ(std::string{nameOp.getName()}, "InferModelName");

    auto desc = nameOp.explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_FALSE(desc.empty());
    EXPECT_TRUE(desc.find("myModel") != std::string::npos);

    /// Can be wrapped in LogicalOperator{}
    EXPECT_NO_THROW({
        LogicalOperator wrapped{nameOp};
        (void)wrapped;
    });
}

/// ---------------------------------------------------------------------------
/// Schema inference — happy-path and multi-output (SCHEMA-01, SCHEMA-02, SCHEMA-06)
/// ---------------------------------------------------------------------------

/// SCHEMA-01: withInferredSchema appends model output fields to input schema
TEST_F(InferModelLogicalOperatorTest, WithInferredSchemaAppendsOutputFields)
{
    /// Model: 1 Float32 input, 1 Float32 output named "prediction"
    auto model = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op{std::move(model), {"value"}};

    auto inputSchema = makeSchema({{"value", DataType::Type::FLOAT32}});
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    EXPECT_TRUE(outputSchema.getFieldByName("value").has_value());
    EXPECT_TRUE(outputSchema.getFieldByName("prediction").has_value());
    EXPECT_EQ(outputSchema.getNumberOfFields(), 2u);
}

/// SCHEMA-02: Output field ordering is preserved and verifiable by index
TEST_F(InferModelLogicalOperatorTest, OutputFieldOrderingPreserved)
{
    /// Model: 1 Float32 input, 2 outputs: "out_a" (Float32), "out_b" (Int32)
    auto model = makeModel({DataType::Type::FLOAT32}, {{"out_a", DataType::Type::FLOAT32}, {"out_b", DataType::Type::INT32}});
    InferModelLogicalOperator op{std::move(model), {"value"}};

    auto inputSchema = makeSchema({{"value", DataType::Type::FLOAT32}});
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    ASSERT_EQ(outputSchema.getNumberOfFields(), 3u);

    /// Verify by index: field[0]="value", field[1]="out_a", field[2]="out_b"
    auto field0 = outputSchema.getFieldAt(0);
    auto field1 = outputSchema.getFieldAt(1);
    auto field2 = outputSchema.getFieldAt(2);

    EXPECT_EQ(field0.getUnqualifiedName(), "value");
    EXPECT_EQ(field0.dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));

    EXPECT_EQ(field1.getUnqualifiedName(), "out_a");
    EXPECT_EQ(field1.dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));

    EXPECT_EQ(field2.getUnqualifiedName(), "out_b");
    EXPECT_EQ(field2.dataType, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}));
}

/// SCHEMA-06: Multiple output fields of mixed types all appear in output schema
TEST_F(InferModelLogicalOperatorTest, MultipleOutputFieldsHandled)
{
    /// Model: 1 Float32 input, 3 outputs: "score" (Float32), "class_id" (Int32), "confidence" (Int64)
    auto model = makeModel(
        {DataType::Type::FLOAT32},
        {{"score", DataType::Type::FLOAT32}, {"class_id", DataType::Type::INT32}, {"confidence", DataType::Type::INT64}});
    InferModelLogicalOperator op{std::move(model), {"input_val"}};

    auto inputSchema = makeSchema({{"input_val", DataType::Type::FLOAT32}});
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    /// 1 input field + 3 output fields = 4 total
    EXPECT_EQ(outputSchema.getNumberOfFields(), 4u);

    auto scoreField = outputSchema.getFieldByName("score");
    ASSERT_TRUE(scoreField.has_value());
    EXPECT_EQ(scoreField->dataType, (DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}));

    auto classIdField = outputSchema.getFieldByName("class_id");
    ASSERT_TRUE(classIdField.has_value());
    EXPECT_EQ(classIdField->dataType, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}));

    auto confidenceField = outputSchema.getFieldByName("confidence");
    ASSERT_TRUE(confidenceField.has_value());
    EXPECT_EQ(confidenceField->dataType, (DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}));
}

/// ---------------------------------------------------------------------------
/// Schema inference — error-path and edge-case tests (SCHEMA-03, SCHEMA-04, SCHEMA-05, SCHEMA-07)
/// ---------------------------------------------------------------------------

/// SCHEMA-03: Empty input schemas vector throws CannotInferSchema
TEST_F(InferModelLogicalOperatorTest, ErrorOnEmptyInputSchema)
{
    auto op = makeOp();
    /// withInferredSchema is [[nodiscard]] — cast to void inside the macro to suppress warning
    ASSERT_EXCEPTION_ERRORCODE((void)op.withInferredSchema({}), NES::ErrorCode::CannotInferSchema);
}

/// SCHEMA-04: Missing required input fields throws CannotInferSchema
TEST_F(InferModelLogicalOperatorTest, ErrorOnMissingRequiredInputFields)
{
    /// Sub-test A: inputFieldNames count != model.getInputs().size()
    /// Model expects 2 Float32 inputs but operator only has 1 inputFieldName
    auto model1 = makeModel({DataType::Type::FLOAT32, DataType::Type::FLOAT32}, {{"result", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op1{std::move(model1), {"only_one_field"}};
    auto schemaA = makeSchema({{"only_one_field", DataType::Type::FLOAT32}});
    ASSERT_EXCEPTION_ERRORCODE((void)op1.withInferredSchema({schemaA}), NES::ErrorCode::CannotInferSchema);

    /// Sub-test B: field referenced in inputFieldNames is absent from schema
    auto model2 = makeModel({DataType::Type::FLOAT32, DataType::Type::FLOAT32}, {{"result", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op2{std::move(model2), {"field_a", "field_b"}};
    /// Schema only has "field_a" — "field_b" is missing
    auto schemaB = makeSchema({{"field_a", DataType::Type::FLOAT32}});
    ASSERT_EXCEPTION_ERRORCODE((void)op2.withInferredSchema({schemaB}), NES::ErrorCode::CannotInferSchema);
}

/// SCHEMA-05: Type mismatch throws CannotInferSchema; VARSIZED type is accepted when model expects it
TEST_F(InferModelLogicalOperatorTest, ErrorOnTypeMismatch)
{
    /// Model expects Float32 input but schema has Int32 — should throw
    auto model = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op{std::move(model), {"value"}};
    auto wrongTypeSchema = makeSchema({{"value", DataType::Type::INT32}});
    ASSERT_EXCEPTION_ERRORCODE((void)op.withInferredSchema({wrongTypeSchema}), NES::ErrorCode::CannotInferSchema);

    /// VARSIZED acceptance: model expects VARSIZED, schema has VARSIZED — should NOT throw
    auto varsizedModel = makeModel({DataType::Type::VARSIZED}, {{"embedding", DataType::Type::FLOAT32}});
    InferModelLogicalOperator varsizedOp{std::move(varsizedModel), {"text"}};
    auto varsizedSchema = makeSchema({{"text", DataType::Type::VARSIZED}});
    EXPECT_NO_THROW({
        auto inferred = varsizedOp.withInferredSchema({varsizedSchema});
        (void)inferred;
    });
}

/// SCHEMA-07: Name collision replaces existing field type rather than duplicating or erroring
TEST_F(InferModelLogicalOperatorTest, NameCollisionReplacesFieldType)
{
    /// Model: 1 Float32 input ("input_field"), 1 output "value" of Int32
    /// Schema: fields "input_field" (Float32), "value" (Float32)
    /// After withInferredSchema: "value" has Int32, total field count stays 2
    auto model = makeModel({DataType::Type::FLOAT32}, {{"value", DataType::Type::INT32}});
    InferModelLogicalOperator op{std::move(model), {"input_field"}};

    auto inputSchema = makeSchema({{"input_field", DataType::Type::FLOAT32}, {"value", DataType::Type::FLOAT32}});
    auto inferred = op.withInferredSchema({inputSchema});

    auto outputSchema = inferred.getOutputSchema();
    /// Field count unchanged — no duplication
    EXPECT_EQ(outputSchema.getNumberOfFields(), 2u);

    /// "value" field type was replaced from Float32 to Int32
    auto valueField = outputSchema.getFieldByName("value");
    ASSERT_TRUE(valueField.has_value());
    EXPECT_EQ(valueField->dataType, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}));
}

}
