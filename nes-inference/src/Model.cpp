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

#include <Model.hpp>

#include <cstddef>
#include <filesystem>
#include <functional>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <Inference.hpp>

namespace NES
{

namespace detail
{
struct ReflectedImportedModel
{
    std::optional<std::string> mlir;
    std::optional<std::string> functionName;
    std::optional<std::vector<size_t>> inputShape;
    std::optional<std::vector<size_t>> outputShape;
};

struct
    ReflectedRegisteredModel /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding optionals of vector-backed types trip the check; no real escape
{
    std::optional<std::string> name;
    std::optional<std::string> path;
    std::optional<Reflected> imported;
    std::optional<Schema<UnqualifiedUnboundField, Ordered> > inputs;
    std::optional<Schema<UnqualifiedUnboundField, Ordered> > outputs;
};
}

Reflected Reflector<ImportedModel>::operator()(const ImportedModel& model) const
{
    const auto data = model.getData();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for textual MLIR serialization
    std::string mlir(reinterpret_cast<const char*>(data.data()), data.size());
    return reflect(detail::ReflectedImportedModel{
        .mlir = std::make_optional(std::move(mlir)),
        .functionName = std::make_optional(model.getFunctionName()),
        .inputShape = std::make_optional(model.getInputShape()),
        .outputShape = std::make_optional(model.getOutputShape())});
}

ImportedModel Unreflector<ImportedModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const {
    auto [mlirOpt, functionName, inputShape, outputShape] = context.unreflect<detail::ReflectedImportedModel>(rfl);
    const auto mlir = mlirOpt.value_or(std::string{});
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char-to-byte for buffer construction
    const auto* mlirBytes = reinterpret_cast<const std::byte*>(mlir.data());
    return ImportedModel{
        detail::RefCountedByteBuffer::fromBytes({mlirBytes, mlir.size()}),
        functionName.value_or(std::string{}),
        inputShape.value_or(std::vector<size_t>{}),
        outputShape.value_or(std::vector<size_t>{})
    };
}

Reflected Reflector<RegisteredModel>::operator()(const RegisteredModel& model) const
{
    return reflect(detail::ReflectedRegisteredModel{
        .name = std::make_optional(model.getName()),
        .path = std::make_optional(model.getPath().string()),
        .imported = std::make_optional(Reflector<ImportedModel>{}(model.getImported())),
        .inputs = std::make_optional(model.getSchema().inputs),
        .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredModel Unreflector<RegisteredModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const {
    auto [name, path, imported, inputs, outputs] = context.unreflect<detail::ReflectedRegisteredModel>(rfl);
    if (!name.has_value() || !path.has_value() || !imported.has_value() || !inputs.has_value() || !outputs.has_value()) {
        throw CannotDeserialize("Failed to deserialize RegisteredModel");
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    /// Schema's user-declared destructor suppresses its implicit move ctor; std::move on the field initializers
    /// would just rebind to copy-from-const-ref. Pass by value.
    ModelSchema schema{.inputs = inputs.value(), .outputs = outputs.value()};
    return RegisteredModel{
        std::move(name).value(),
        std::filesystem::path(std::move(path).value()),
        context.unreflect<ImportedModel>(imported.value()),
        std::move(schema)};
}

RegisteredModel RegisteredModel::create(std::string name, std::filesystem::path path, ModelSchema schema) {
    if (!std::filesystem::exists(path)) {
        throw InvalidStatement("Model path does not exist: {}", path);
    }

    /// Coordinator-side: only import the model — signature is scraped during
    /// import. The compile step runs later on the worker, during lowering.
    auto imported = importModel(path);
    if (!imported) {
        throw CannotLoadModel("Failed to import model '{}': {}", name, imported.error().message);
    }

    /// The runtime is f32-only and the physical operator writes/reads one f32
    /// slot per non-VARSIZED field. So declared fields must be FLOAT32, except
    /// for the bulk-byte VARSIZED escape hatch — a single field that mirrors
    /// the whole tensor verbatim. Validating this here makes
    /// `model ↔ modelSchema` compatibility an invariant downstream.
    const auto validateSide
            = [&](const Schema<UnqualifiedUnboundField, Ordered> &fields, const std::vector<size_t> &tensorShape,
                  std::string_view role) {
        bool hasVarsized = false;
        for (const auto &field: fields) {
            const auto type = field.getDataType().type;
            if (type != DataType::Type::FLOAT32 && type != DataType::Type::VARSIZED) {
                throw CannotLoadModel(
                    "Model '{}' {} field '{}': type must be FLOAT32 or VARSIZED", name, role,
                    field.getFullyQualifiedName());
            }
            if (type == DataType::Type::VARSIZED) {
                hasVarsized = true;
            }
        }
        if (hasVarsized && fields.size() != 1) {
            throw CannotLoadModel("Model '{}' {}: VARSIZED requires exactly one {} field but got {}", name, role, role,
                                  fields.size());
        }
        if (!hasVarsized) {
            const size_t elementCount = std::accumulate(tensorShape.begin(), tensorShape.end(), size_t{1},
                                                        std::multiplies<>());
            if (fields.size() != elementCount) {
                throw CannotLoadModel(
                    "Model '{}' {}: declared {} field(s) but tensor has {} element(s)", name, role, fields.size(),
                    elementCount);
            }
        }
    };
    validateSide(schema.inputs, imported->getInputShape(), "input");
    validateSide(schema.outputs, imported->getOutputShape(), "output");

    return {name, std::move(path), std::move(*imported), std::move(schema)};
}

}
