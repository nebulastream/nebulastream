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
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES::InferModel
{

/**
 * @brief Infer model operator
 */
class LogicalInferModelNameOperator : public LogicalOperatorConcept
{
    std::string modelName;
    std::vector<LogicalFunction> inputFields;
    LogicalOperator child;
    std::vector<OriginId> inputOriginIds;
    std::vector<OriginId> outputOriginIds;

public:
    LogicalInferModelNameOperator(std::string modelName, std::vector<LogicalFunction> inputFields)
        : modelName(std::move(modelName)), inputFields(std::move(inputFields))
    {
    }

    [[nodiscard]] const std::string& getModelName() const { return modelName; }
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return {child}; }
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override
    {
        PRECONDITION(children.size() == 1, "Expected exactly one child");
        auto copy = *this;
        copy.child = std::move(children.front());
        return copy;
    }

    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override
    {
        if (const auto* casted = dynamic_cast<const LogicalInferModelNameOperator*>(&rhs))
        {
            return modelName == casted->modelName && inputFields == casted->inputFields && child == casted->child
                && inputOriginIds == casted->inputOriginIds && outputOriginIds == casted->outputOriginIds;
        }
        return false;
    }

    [[nodiscard]] const std::vector<LogicalFunction>& getInputFields() const { return inputFields; }
    [[nodiscard]] std::string_view getName() const noexcept override { return "InferenceModelName"; }
    [[nodiscard]] SerializableOperator serialize() const override
    {
        SerializableLogicalOperator proto;

        proto.set_operator_type(getName());
        auto* traitSetProto = proto.mutable_trait_set();
        for (const auto& trait : getTraitSet())
        {
            *traitSetProto->add_traits() = trait.serialize();
        }

        for (const auto& originList : getInputOriginIds())
        {
            auto* olist = proto.add_input_origin_lists();
            for (auto originId : originList)
            {
                olist->add_origin_ids(originId.getRawValue());
            }
        }

        for (auto outId : getOutputOriginIds())
        {
            proto.add_output_origin_ids(outId.getRawValue());
        }

        auto* outSch = proto.mutable_output_schema();
        SchemaSerializationUtil::serializeSchema(Schema{}, outSch);



        SerializableOperator serializableOperator;
        serializableOperator.set_operator_id(id.getRawValue());

        FunctionList funcList;
        for (const auto & inputField : inputFields)
        {
            *funcList.add_functions() = inputField.serialize();
        }
        (*serializableOperator.mutable_config())["inputFields"] = Configurations::descriptorConfigTypeToProto(funcList);

        for (auto& child : getChildren())
        {
            serializableOperator.add_children_ids(child.getId().getRawValue());
        }

        const Configurations::DescriptorConfig::ConfigType modelNameConfig = modelName;
        (*serializableOperator.mutable_config())[ConfigParameters::MODEL_NAME] = descriptorConfigTypeToProto(modelNameConfig);

        serializableOperator.mutable_operator_()->CopyFrom(proto);
        return serializableOperator;
    }
    [[nodiscard]] TraitSet getTraitSet() const override { return {}; }
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }
    [[nodiscard]] Schema getOutputSchema() const override
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }
    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {inputOriginIds}; }

    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return outputOriginIds; }

    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> originIds) const override
    {
        PRECONDITION(originIds.size() == 1, "Expected exactly one set of origin ids");
        auto copy = *this;
        copy.inputOriginIds = std::move(originIds.front());
        return copy;
    }

    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> originIds) const override
    {
        auto copy = *this;
        copy.outputOriginIds = std::move(originIds);
        return copy;
    }

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema>) const override
    {
        throw CannotInferSchema("Schema is not available on the INFER_MODEL_NAME. The Infere operator has to be resolved first.");
    }

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> MODEL_NAME{
            "modelName",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return NES::Configurations::DescriptorConfig::tryGet(MODEL_NAME, config); }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(MODEL_NAME);
    };
};
}
