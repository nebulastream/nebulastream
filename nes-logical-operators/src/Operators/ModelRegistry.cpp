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
struct RegisteredModelNESType
{
    using ValueType = RegisteredModel;
    bool operator==(const RegisteredModelNESType&) const = default;
};

struct RegisteredModelTypeTraitImpl
{
    [[nodiscard]] static ModelSchema getSchema(const NESValue& val) { return val.getAs<RegisteredModel>().getSchema(); }

    [[nodiscard]] static std::expected<LogicalOperator, Exception>
    createInferenceOperator(const NESValue& val, LogicalOperator child)
    {
        const auto* modelPtr = val.tryGetAs<RegisteredModel>();
        if (modelPtr == nullptr)
        {
            return std::unexpected{TypeInferenceException("Invalid RegisteredModel value representation")};
        }
        auto op = TypedLogicalOperator<InferModelLogicalOperator>{*modelPtr};
        return LogicalOperator{op.withChildrenUnsafe({std::move(child)})};
    }
};

struct DelegatingModelBackendTypeTraitImpl
{
    [[nodiscard]] static ModelSchema getSchema(const NESValue& outerVal)
    {
        const auto& innerVal = outerVal.getAs<NESValue>();
        const auto* typeTrait = innerVal.getType().tryGetTypeTrait<MLModelTypeTrait>();
        PRECONDITION(typeTrait != nullptr, "Constructed model value must implement MLModelTypeTrait");
        return typeTrait->getSchema(innerVal);
    }

    [[nodiscard]] static std::expected<LogicalOperator, Exception>
    createInferenceOperator(const NESValue& outerVal, LogicalOperator child)
    {
        const auto* innerVal = outerVal.tryGetAs<NESValue>();
        if (innerVal == nullptr)
        {
            return std::unexpected{TypeInferenceException("Invalid model value wrapper")};
        }
        const auto* typeTrait = innerVal->getType().tryGetTypeTrait<MLModelTypeTrait>();
        if (typeTrait == nullptr)
        {
            return std::unexpected{TypeInferenceException("Model value does not implement MLModelTypeTrait")};
        }
        return typeTrait->createInferenceOperator(*innerVal, std::move(child));
    }
};

struct ModelBackendNESType
{
    Identifier backendName;
    ModelFactoryFn factory;

    bool operator==(const ModelBackendNESType& other) const { return backendName == other.backendName; }

    [[nodiscard]] std::expected<NESValue, Exception> construct(const NESValueParameters& params) const
    {
        auto constructed = factory(params);
        if (!constructed.has_value())
        {
            return constructed;
        }
        if (!constructed->getType().hasTypeTrait<MLModelTypeTrait>())
        {
            return std::unexpected{
                TypeInferenceException("Model backend '{}' did not produce an ML_MODEL value", backendName)};
        }
        return constructed;
    }
};

struct ModelBackendFunctor
{
    Identifier backendName;
    ModelFactoryFn factory;

    [[nodiscard]] const ModelFactoryFn& getRegistryEntry() const noexcept { return factory; }
    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments() { return ExpectedNESTypeArguments{}; }
    bool operator==(const ModelBackendFunctor& other) const { return backendName == other.backendName; }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters&) const
    {
        return NESType{ModelBackendNESType{.backendName = backendName, .factory = factory}}
            .withTypeTrait(MLModelTypeTrait{DelegatingModelBackendTypeTraitImpl{}});
    }
};

NESType registeredModelNESType()
{
    static const NESType instance
        = NESType{RegisteredModelNESType{}}.withTypeTrait(MLModelTypeTrait{RegisteredModelTypeTraitImpl{}});
    return instance;
}

std::expected<std::string, Exception> extractPathString(const NESValue& val)
{
    if (const auto* s = val.tryGetAs<std::string>())
    {
        return *s;
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
            if (const auto* s = constFn->get().getValue().tryGetAs<std::string>())
            {
                return *s;
            }
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
std::expected<Schema<UnqualifiedUnboundField, Ordered>, Exception> extractModelSchema(const NESValue& val, const std::string_view argName)
{
    if (const auto* schema = val.tryGetAs<Schema<UnqualifiedUnboundField, Ordered>>())
    {
        return *schema;
    }
    return std::unexpected{InvalidQuerySyntax("OPENVINO_MODEL {} argument must be a SCHEMA(...)", argName)};
}

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const NESValueParameter<std::string> PATH{
    Identifier::parse("PATH"),
    "Filesystem path to the ONNX model",
    &extractPathString};

const NESValueParameter<Schema<UnqualifiedUnboundField, Ordered>> INPUT{
    Identifier::parse("INPUT"),
    "Model input schema",
    [](const NESValue& val) { return extractModelSchema(val, "INPUT"); }};

const NESValueParameter<Schema<UnqualifiedUnboundField, Ordered>> OUTPUT{
    Identifier::parse("OUTPUT"),
    "Model output schema",
    [](const NESValue& val) { return extractModelSchema(val, "OUTPUT"); }};
/// NOLINTEND(bugprone-throwing-static-initialization)
}

NESValue makeRegisteredModelValue(RegisteredModel model)
{
    return NESValue::create(registeredModelNESType(), std::move(model));
}

std::expected<NESValue, Exception> OpenVinoModelPlugin::create(const NESValueParameters& params)
{
    const auto resolvedExp = createExpectedNESValueArguments(PATH, INPUT, OUTPUT).resolve(params);
    if (!resolvedExp.has_value())
    {
        return std::unexpected{std::move(resolvedExp).error()};
    }
    const auto pathStr = resolvedExp->get(PATH);
    const auto inputSchema = resolvedExp->get(INPUT);
    const auto outputSchema = resolvedExp->get(OUTPUT);

    const std::filesystem::path fsPath{pathStr};
    const auto modelName = fsPath.stem().empty() ? std::string{"inline_openvino_model"} : fsPath.stem().string();

    try
    {
        auto registered = RegisteredModel::create(
            modelName,
            fsPath,
            ModelSchema{
                .inputs = normalizeNonNullableModelFields(inputSchema),
                .outputs = normalizeNonNullableModelFields(outputSchema)});
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
    return NESTypeClass::forTypeTrait<MLModelTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> ModelRegistry::getParametrizedTypeClasses()
{
    return {NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("ML_MODEL"), mlModelTypeClass())};
}

NESFunctor makeModelFunctor(const std::string_view key, ModelFactoryFn factory)
{
    return NESFunctor{ModelBackendFunctor{.backendName = Identifier::parse(std::string{key}), .factory = std::move(factory)}};
}

}
