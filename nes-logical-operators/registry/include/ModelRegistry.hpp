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

#include <concepts>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES
{

class MLModelTypeTrait;

/// Concept fulfilled by concrete ML model backend type-trait implementations wrapped in `MLModelTypeTrait`.
/// Enables `MODEL_INFERENCE` to accept any model satisfying the `ML_MODEL` type class without
/// hardcoding backend model types in the parser or binder.
template <typename T>
concept MLModelTypeTraitConcept = (!std::same_as<std::remove_cvref_t<T>, MLModelTypeTrait>)
    && requires(const std::remove_cvref_t<T>& typeTrait, const NESValue& modelValue, LogicalOperator child) {
           { typeTrait.getSchema(modelValue) } -> std::same_as<ModelSchema>;
           { typeTrait.createInferenceOperator(modelValue, std::move(child)) } -> std::same_as<std::expected<LogicalOperator, Exception>>;
       };

namespace detail
{
struct ErasedMLModelTypeTrait
{
    virtual ~ErasedMLModelTypeTrait() = default;
    [[nodiscard]] virtual ModelSchema getSchema(const NESValue& modelValue) const = 0;
    [[nodiscard]] virtual std::expected<LogicalOperator, Exception>
    createInferenceOperator(const NESValue& modelValue, LogicalOperator child) const = 0;
};

template <MLModelTypeTraitConcept T>
struct MLModelTypeTraitModel final : ErasedMLModelTypeTrait
{
    T impl;

    explicit MLModelTypeTraitModel(T impl) : impl(std::move(impl)) { }

    [[nodiscard]] ModelSchema getSchema(const NESValue& modelValue) const override { return impl.getSchema(modelValue); }

    [[nodiscard]] std::expected<LogicalOperator, Exception>
    createInferenceOperator(const NESValue& modelValue, LogicalOperator child) const override
    {
        return impl.createInferenceOperator(modelValue, std::move(child));
    }
};
}

/// Open `TypeTrait` wrapper implemented by all ML model `NESType`s (e.g. `OPENVINO_MODEL`, catalog `RegisteredModel`s,
/// or external inference backend plugins such as an IREE model plugin).
class MLModelTypeTrait
{
    std::shared_ptr<const detail::ErasedMLModelTypeTrait> self;

public:
    template <MLModelTypeTraitConcept T>
    explicit MLModelTypeTrait(T typeTraitImpl)
        : self(std::make_shared<detail::MLModelTypeTraitModel<std::remove_cvref_t<T>>>(std::move(typeTraitImpl)))
    {
    }

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept
    {
        using CleanT = std::remove_cvref_t<T>;
        if (const auto* model = dynamic_cast<const detail::MLModelTypeTraitModel<CleanT>*>(self.get()))
        {
            return &model->impl;
        }
        return nullptr;
    }

    [[nodiscard]] ModelSchema getSchema(const NESValue& modelValue) const { return self->getSchema(modelValue); }

    [[nodiscard]] std::expected<LogicalOperator, Exception>
    createInferenceOperator(const NESValue& modelValue, LogicalOperator child) const
    {
        return self->createInferenceOperator(modelValue, std::move(child));
    }
};

using ModelFactoryFn = std::function<std::expected<NESValue, Exception>(const NESValueParameters&)>;

/// Wraps a `RegisteredModel` (either loaded inline via `OPENVINO_MODEL(...)` or looked up from `ModelCatalog`)
/// into a `NESValue` whose `NESType` implements `MLModelTypeTrait` (`ML_MODEL`).
[[nodiscard]] NESValue makeRegisteredModelValue(RegisteredModel model);

/// Built-in factory entry for `OPENVINO_MODEL(PATH, INPUT, OUTPUT)`.
struct OpenVinoModelPlugin
{
    static std::expected<NESValue, Exception> create(const NESValueParameters& params);
};

class ModelRegistry : public RuntimeRegistry<ModelRegistry, std::string, ModelFactoryFn, /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static ModelRegistry& instance();
    static NESTypeClass mlModelTypeClass();
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();
};

NESFunctor makeModelFunctor(std::string_view key, ModelFactoryFn factory);

}
