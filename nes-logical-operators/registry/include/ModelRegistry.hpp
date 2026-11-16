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

#include <expected>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES
{

/// Open trait implemented by all ML model `NESType`s (e.g. `OPENVINO_MODEL`, catalog `RegisteredModel`s,
/// or external inference backend plugins such as an IREE model plugin).
/// Enables `MODEL_INFERENCE` to accept any model satisfying the `ML_MODEL` type class without
/// hardcoding backend model types in the parser or binder.
struct MLModelTrait
{
    std::function<ModelSchema(const NESValue&)> getSchema;
    std::function<std::expected<LogicalOperator, Exception>(const NESValue&, LogicalOperator child)> createInferenceOperator;
};

using ModelFactoryFn = std::function<std::expected<NESValue, Exception>(const NESValueParameters&)>;

/// Wraps a `RegisteredModel` (either loaded inline via `OPENVINO_MODEL(...)` or looked up from `ModelCatalog`)
/// into a `NESValue` whose `NESType` implements `MLModelTrait` (`ML_MODEL`).
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
