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

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Logical operator that persists rows to a binary file while passing them downstream unchanged.
class ReplayStoreLogicalOperator : public ManagedByOperator
{
public:
    ReplayStoreLogicalOperator() : ManagedByOperator(WeakLogicalOperator{}) { }

    explicit ReplayStoreLogicalOperator(WeakLogicalOperator self, DescriptorConfig::Config validatedConfig)
        : ManagedByOperator(std::move(self)), config(std::move(validatedConfig)) { }

    explicit ReplayStoreLogicalOperator(DescriptorConfig::Config validatedConfig)
        : ManagedByOperator(WeakLogicalOperator{}), config(std::move(validatedConfig)) { }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] static std::string_view getName() noexcept;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] ReplayStoreLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] ReplayStoreLogicalOperator withTraitSet(TraitSet ts) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] bool operator==(const ReplayStoreLogicalOperator& rhs) const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;
    [[nodiscard]] ReplayStoreLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    [[nodiscard]] const DescriptorConfig::Config& getConfig() const { return config; }

    [[nodiscard]] ReplayStoreLogicalOperator withConfig(DescriptorConfig::Config validatedConfig) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> STORE_NAME{
            "store_name",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& cfg) { return DescriptorConfig::tryGet(STORE_NAME, cfg); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(STORE_NAME);
    };

    static DescriptorConfig::Config validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs);

private:
    static constexpr std::string_view NAME = "ReplayStore";
    std::vector<LogicalOperator> children;
    TraitSet traitSet;

    DescriptorConfig::Config config;
};

template <>
struct Reflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<ReplayStoreLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>
{
    TypedLogicalOperator<ReplayStoreLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<ReplayStoreLogicalOperator>);
}

namespace NES::detail
{
struct ReflectedStoreLogicalOperator
{
    DescriptorConfig::Config config;
};
}
