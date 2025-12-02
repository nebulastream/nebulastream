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

#pragma once
#include <string>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class InferModelNameLogicalOperator : public OriginIdAssigner
{
    std::string modelName;
    std::vector<LogicalFunction> inputFields;
    LogicalOperator child;

public:
    InferModelNameLogicalOperator(std::string modelName, std::vector<LogicalFunction> inputFields)
        : modelName(std::move(modelName)), inputFields(std::move(inputFields))
    {
    }

    [[nodiscard]] const std::string& getModelName() const { return modelName; }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }

    [[nodiscard]] InferModelNameLogicalOperator withChildren(std::vector<LogicalOperator> children) const
    {
        PRECONDITION(children.size() == 1, "Expected exactly one child");
        auto copy = *this;
        copy.child = std::move(children.front());
        return copy;
    }

    [[nodiscard]] bool operator==(const InferModelNameLogicalOperator& rhs) const
    {
        return modelName == rhs.modelName && inputFields == rhs.inputFields && child == rhs.child;
    }

    [[nodiscard]] const std::vector<LogicalFunction>& getInputFields() const { return inputFields; }

    [[nodiscard]] std::string_view getName() const noexcept { return NAME; }

    void serialize(SerializableOperator& serializableOperator) const
    {
        SerializableLogicalOperator proto;

        proto.set_operator_type(NAME);

        for (const auto& inputSchema : getInputSchemas())
        {
            auto* schProto = proto.add_input_schemas();
            SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
        }

        auto* outSch = proto.mutable_output_schema();
        SchemaSerializationUtil::serializeSchema(Schema{}, outSch);

        for (const auto& child : getChildren())
        {
            serializableOperator.add_children_ids(child.getId().getRawValue());
        }

        FunctionList funcList;
        for (const auto& inputField : inputFields)
        {
            *funcList.add_functions() = inputField.serialize();
        }
        (*serializableOperator.mutable_config())["inputFields"] = descriptorConfigTypeToProto(funcList);

        for (auto& child : getChildren())
        {
            serializableOperator.add_children_ids(child.getId().getRawValue());
        }

        const DescriptorConfig::ConfigType modelNameConfig = modelName;
        (*serializableOperator.mutable_config())[ConfigParameters::MODEL_NAME] = descriptorConfigTypeToProto(modelNameConfig);

        serializableOperator.mutable_operator_()->CopyFrom(proto);
    }

    [[nodiscard]] TraitSet getTraitSet() const { return {traitSet}; }

    [[nodiscard]] InferModelNameLogicalOperator withTraitSet(TraitSet traitSet) const
    {
        auto copy = *this;
        copy.traitSet = traitSet;
        return copy;
    }

    [[nodiscard]] std::vector<Schema> getInputSchemas() const
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }

    [[nodiscard]] Schema getOutputSchema() const
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }

    [[nodiscard]] InferModelNameLogicalOperator withInferredSchema(std::vector<Schema>) const
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> MODEL_NAME{
            "modelName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MODEL_NAME, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(MODEL_NAME);
    };

private:
    static constexpr std::string_view NAME = "InferModelName";
    TraitSet traitSet;
};
}
