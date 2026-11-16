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

#include <ModelRegistry.hpp>

#include <array>
#include <expected>
#include <filesystem>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES
{

namespace
{
struct RegisteredModelTypeTag
{
    bool operator==(const RegisteredModelTypeTag&) const = default;
};

struct ModelBackendDescriptor
{
    Identifier backendName;
    ModelFactoryFn factory;
    bool operator==(const ModelBackendDescriptor& other) const { return backendName == other.backendName; }
};

NESType registeredModelNESType()
{
    static const NESType instance
        = NESType::createUnconstructible<RegisteredModelTypeTag, RegisteredModel>(RegisteredModelTypeTag{})
              .withTrait(MLModelTrait{
                  .getSchema = [](const NESValue& val) -> ModelSchema { return val.getAs<RegisteredModel>().getSchema(); },
                  .createInferenceOperator =
                      [](const NESValue& val, LogicalOperator child) -> std::expected<LogicalOperator, Exception>
                  {
                      const auto* modelPtr = val.tryGetAs<RegisteredModel>();
                      if (modelPtr == nullptr)
                      {
                          return std::unexpected{TypeInferenceException("Invalid RegisteredModel value representation")};
                      }
                      auto op = TypedLogicalOperator<InferModelLogicalOperator>{*modelPtr};
                      return LogicalOperator{op.withChildrenUnsafe({std::move(child)})};
                  }});
    return instance;
}

std::expected<std::string, Exception> extractPathString(const NESValue& val)
{
    if (const auto* s = val.tryGetAs<std::string>())
    {
        return *s;
    }
    if (const auto* lit = val.tryGetAs<ScalarLiteralValue>())
    {
        if (const auto* s = std::get_if<std::string>(lit))
        {
            return *s;
        }
    }
    if (const auto* cfg = val.tryGetAs<ConfigLiteral>())
    {
        if (const auto* s = std::get_if<std::string>(cfg))
        {
            return *s;
        }
    }
    if (const auto* fn = val.tryGetAs<LogicalFunction>())
    {
        if (const auto constFn = fn->tryGetAs<ConstantValueLogicalFunction>())
        {
            auto str = constFn->get().getConstantValue();
            if (constFn->get().getDataType().type == DataType::Type::UNDEFINED && str.size() >= 2 && str.front() == '\''
                && str.back() == '\'')
            {
                return str.substr(1, str.size() - 2);
            }
            return str;
        }
    }
    return std::unexpected{InvalidQuerySyntax("Model PATH argument must be a string literal")};
}

Schema<UnqualifiedUnboundField, Ordered> normalizeNonNullableModelFields(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    auto normalized = schema
        | std::views::transform(
                          [](const UnqualifiedUnboundField& field)
                          {
                              auto dt = field.getDataType();
                              dt.nullable = false;
                              return UnqualifiedUnboundField{field.getFullyQualifiedName(), dt};
                          })
        | std::ranges::to<std::vector>();
    return Schema<UnqualifiedUnboundField, Ordered>{std::move(normalized)};
}
}

NESValue makeRegisteredModelValue(RegisteredModel model)
{
    return NESValue::create(registeredModelNESType(), std::move(model));
}

std::expected<NESValue, Exception> OpenVinoModelPlugin::create(const NESValueParameters& params)
{
    static const std::array<QualifiedIdentifier, 3> formalParams{
        QualifiedIdentifier::parse("PATH"),
        QualifiedIdentifier::parse("INPUT"),
        QualifiedIdentifier::parse("OUTPUT")};

    const auto boundExp = params.bindAgainst(formalParams);
    if (!boundExp.has_value())
    {
        return std::unexpected{InvalidQuerySyntax(
            "OPENVINO_MODEL expects arguments (PATH, INPUT, OUTPUT): {}", boundExp.error().what())};
    }
    const auto& bound = *boundExp;

    const auto pathVal = bound.getFieldByName(formalParams[0]).value().getValue();
    const auto inputVal = bound.getFieldByName(formalParams[1]).value().getValue();
    const auto outputVal = bound.getFieldByName(formalParams[2]).value().getValue();

    const auto pathExp = extractPathString(pathVal);
    if (!pathExp.has_value())
    {
        return std::unexpected{pathExp.error()};
    }
    const auto* inputSchema = inputVal.tryGetAs<Schema<UnqualifiedUnboundField, Ordered>>();
    if (inputSchema == nullptr)
    {
        return std::unexpected{InvalidQuerySyntax("OPENVINO_MODEL INPUT argument must be a SCHEMA(...)")};
    }
    const auto* outputSchema = outputVal.tryGetAs<Schema<UnqualifiedUnboundField, Ordered>>();
    if (outputSchema == nullptr)
    {
        return std::unexpected{InvalidQuerySyntax("OPENVINO_MODEL OUTPUT argument must be a SCHEMA(...)")};
    }

    const std::filesystem::path fsPath{*pathExp};
    const auto modelName = fsPath.stem().empty() ? std::string{"inline_openvino_model"} : fsPath.stem().string();

    try
    {
        auto registered = RegisteredModel::create(
            modelName,
            fsPath,
            ModelSchema{
                .inputs = normalizeNonNullableModelFields(*inputSchema),
                .outputs = normalizeNonNullableModelFields(*outputSchema)});
        return makeRegisteredModelValue(std::move(registered));
    }
    catch (const Exception& ex)
    {
        return std::unexpected{ex};
    }
}

ModelRegistry& ModelRegistry::instance()
{
    static ModelRegistry inst;
    return inst;
}

NESTypeClass ModelRegistry::mlModelTypeClass()
{
    return NESTypeClass::forTrait<MLModelTrait>();
}

std::vector<NamedNESParametrizedTypeClass> ModelRegistry::getParametrizedTypeClasses()
{
    return {NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("ML_MODEL"), mlModelTypeClass())};
}

NESFunctor makeModelFunctor(const std::string_view key, ModelFactoryFn factory)
{
    const auto backendId = Identifier::parse(std::string{key});
    return NESFunctor::nullary<ModelFactoryFn>(
        std::move(factory),
        [backendId](const ModelFactoryFn& fn) -> std::expected<NESType, Exception>
        {
            return NESType::create<ModelBackendDescriptor, NESValue>(
                       ModelBackendDescriptor{.backendName = backendId, .factory = fn},
                       [](const ModelBackendDescriptor& desc, const NESValueParameters& params) -> std::expected<NESValue, Exception>
                       { return desc.factory(params); })
                .withTrait(MLModelTrait{
                    .getSchema =
                        [](const NESValue& outerVal) -> ModelSchema
                    {
                        const auto& innerVal = outerVal.getAs<NESValue>();
                        const auto* trait = innerVal.getType().tryGetTrait<MLModelTrait>();
                        PRECONDITION(trait != nullptr, "Constructed model value must implement MLModelTrait");
                        return trait->getSchema(innerVal);
                    },
                    .createInferenceOperator =
                        [](const NESValue& outerVal, LogicalOperator child) -> std::expected<LogicalOperator, Exception>
                    {
                        const auto* innerVal = outerVal.tryGetAs<NESValue>();
                        if (innerVal == nullptr)
                        {
                            return std::unexpected{TypeInferenceException("Invalid model value wrapper")};
                        }
                        const auto* trait = innerVal->getType().tryGetTrait<MLModelTrait>();
                        if (trait == nullptr)
                        {
                            return std::unexpected{TypeInferenceException("Model value does not implement MLModelTrait")};
                        }
                        return trait->createInferenceOperator(*innerVal, std::move(child));
                    }});
        });
}

}
