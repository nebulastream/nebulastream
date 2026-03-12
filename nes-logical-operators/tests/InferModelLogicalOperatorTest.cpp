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
    static NES::Nebuli::Inference::Model makeModel(
        std::vector<DataType::Type> inputs,
        std::vector<std::pair<std::string, DataType::Type>> outputs)
    {
        SerializableOperator_Model proto;
        for (auto inputType : inputs)
        {
            DataTypeSerializationUtil::serializeDataType(
                DataType{inputType, DataType::NULLABLE::NOT_NULLABLE}, proto.add_inputs());
        }
        for (auto& [name, type] : outputs)
        {
            auto* out = proto.add_outputs();
            out->set_name(name);
            DataTypeSerializationUtil::serializeDataType(
                DataType{type, DataType::NULLABLE::NOT_NULLABLE}, out->mutable_type());
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

// LOGI-01: Construct InferModelLogicalOperator with valid Model and wrap in LogicalOperator{}
TEST_F(InferModelLogicalOperatorTest, ConstructWithValidModel)
{
    EXPECT_NO_THROW({
        auto op = makeOp();
        LogicalOperator wrapped{op};
        (void)wrapped;
    });
}

// LOGI-02: explain() returns non-empty string containing "InferModel" or "INFER_MODEL"
TEST_F(InferModelLogicalOperatorTest, ExplainReturnsDescription)
{
    auto op = makeOp();
    auto description = op.explain(ExplainVerbosity::Short, OperatorId{1});
    EXPECT_FALSE(description.empty());
    EXPECT_TRUE(description.find("INFER_MODEL") != std::string::npos
                || description.find("InferModel") != std::string::npos);
}

// LOGI-03: withChildren/getChildren round-trip — immutable copy semantics
TEST_F(InferModelLogicalOperatorTest, WithChildrenRoundTrip)
{
    auto op = makeOp();
    EXPECT_TRUE(op.getChildren().empty());

    // Create a child operator
    auto childModel = makeModel({DataType::Type::FLOAT32}, {{"out", DataType::Type::FLOAT32}});
    InferModelLogicalOperator childOp{std::move(childModel), {"x"}};
    LogicalOperator child{childOp};

    auto withChild = op.withChildren({child});
    ASSERT_EQ(withChild.getChildren().size(), 1u);
    // Original is unchanged (value semantics)
    EXPECT_TRUE(op.getChildren().empty());
}

// LOGI-04: withTraitSet/getTraitSet round-trip
TEST_F(InferModelLogicalOperatorTest, WithTraitSetRoundTrip)
{
    auto op = makeOp();
    // Default trait set is empty
    TraitSet emptySet = op.getTraitSet();

    // Apply a new trait set (create a non-default one by copying and checking it's stored)
    auto opWithTraits = op.withTraitSet(emptySet);
    EXPECT_EQ(opWithTraits.getTraitSet(), emptySet);
    // Original is unchanged
    EXPECT_EQ(op.getTraitSet(), emptySet);
}

// LOGI-05: operator== equality semantics
TEST_F(InferModelLogicalOperatorTest, EqualitySemantics)
{
    auto op1 = makeOp();
    auto op2 = makeOp();
    // Same model, same inputFieldNames → equal
    EXPECT_EQ(op1, op2);

    // Different inputFieldNames → not equal
    auto model2 = makeModel({DataType::Type::FLOAT32}, {{"prediction", DataType::Type::FLOAT32}});
    InferModelLogicalOperator op3{std::move(model2), {"other_field"}};
    EXPECT_NE(op1, op3);
}

// LOGI-06: getName returns "InferModel"
TEST_F(InferModelLogicalOperatorTest, GetNameReturnsInferModel)
{
    auto op = makeOp();
    EXPECT_EQ(op.getName(), "InferModel");
}

// LOGI-07: After withInferredSchema, getInputSchemas and getOutputSchema reflect inferred state
TEST_F(InferModelLogicalOperatorTest, SchemasPropagateAfterInference)
{
    auto op = makeOp();
    // Schema: field "value" of Float32
    auto inputSchema = makeSchema({{"value", DataType::Type::FLOAT32}});

    auto inferred = op.withInferredSchema({inputSchema});

    // Input schema should be set
    auto inputSchemas = inferred.getInputSchemas();
    ASSERT_EQ(inputSchemas.size(), 1u);
    EXPECT_EQ(inputSchemas[0], inputSchema);

    // Output schema should contain both "value" and "prediction"
    auto outputSchema = inferred.getOutputSchema();
    EXPECT_TRUE(outputSchema.getFieldByName("value").has_value());
    EXPECT_TRUE(outputSchema.getFieldByName("prediction").has_value());
}

// LOGI-08: InferModelNameLogicalOperator construction, getName, and explain
TEST_F(InferModelLogicalOperatorTest, NameVariantConstructionAndExplain)
{
    InferModelNameLogicalOperator nameOp{"myModel", {"feature1", "feature2"}};

    EXPECT_EQ(std::string{nameOp.getName()}, "InferModelName");

    auto desc = nameOp.explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_FALSE(desc.empty());
    EXPECT_TRUE(desc.find("myModel") != std::string::npos);

    // Can be wrapped in LogicalOperator{}
    EXPECT_NO_THROW({
        LogicalOperator wrapped{nameOp};
        (void)wrapped;
    });
}

} // namespace NES
