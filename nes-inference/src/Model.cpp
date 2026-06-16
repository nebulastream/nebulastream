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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>

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
    std::optional<ModelFieldList> inputs;
    std::optional<ModelFieldList> outputs;
};
}

Reflected Reflector<ImportedModel>::operator()(const ImportedModel& model) const
{
    const auto data = model.getData();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-char for textual MLIR serialization
    std::string mlir(reinterpret_cast<const char*>(data.data()), data.size());
    return reflect(
        detail::ReflectedImportedModel{
            .mlir = std::make_optional(std::move(mlir)),
            .functionName = std::make_optional(model.getFunctionName()),
            .inputShape = std::make_optional(model.getInputShape()),
            .outputShape = std::make_optional(model.getOutputShape())});
}

ImportedModel Unreflector<ImportedModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedImportedModel>(rfl);
    const auto mlir = reflected.mlir.value_or(std::string{});
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char-to-byte for buffer construction
    const auto* mlirBytes = reinterpret_cast<const std::byte*>(mlir.data());
    return ImportedModel{
        detail::RefCountedByteBuffer::fromBytes({mlirBytes, mlir.size()}),
        reflected.functionName.value_or(std::string{}),
        reflected.inputShape.value_or(std::vector<size_t>{}),
        reflected.outputShape.value_or(std::vector<size_t>{})};
}

Reflected Reflector<RegisteredModel>::operator()(const RegisteredModel& model) const
{
    return reflect(
        detail::ReflectedRegisteredModel{
            .name = std::make_optional(model.getName()),
            .path = std::make_optional(model.getPath().string()),
            .imported = std::make_optional(Reflector<ImportedModel>{}(model.getImported())),
            .inputs = std::make_optional(model.getSchema().inputs),
            .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredModel Unreflector<RegisteredModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedRegisteredModel>(rfl);
    if (!reflected.name.has_value() || !reflected.path.has_value() || !reflected.imported.has_value() || !reflected.inputs.has_value()
        || !reflected.outputs.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize RegisteredModel");
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    /// Schema's user-declared destructor suppresses its implicit move ctor; std::move on the field initializers
    /// would just rebind to copy-from-const-ref. Pass by value.
    ModelSchema schema{.inputs = reflected.inputs.value(), .outputs = reflected.outputs.value()};
    return RegisteredModel{
        std::move(reflected.name).value(),
        std::filesystem::path(std::move(reflected.path).value()),
        context.unreflect<ImportedModel>(reflected.imported.value()),
        std::move(schema)};
}

}
