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
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{
/// MemorySwap operator, which is lowered to a scan and emit operator capable of swapping the memoryLayoutType.
class MemorySwapLogicalOperator
{
public:
    explicit MemorySwapLogicalOperator(
        const Schema& inputSchema, MemoryLayoutType inputMemoryLayoutType, MemoryLayoutType outputMemoryLayoutType);

    explicit MemorySwapLogicalOperator(
        const Schema& inputSchema,
        MemoryLayoutType inputMemoryLayoutType,
        MemoryLayoutType outputMemoryLayoutType,
        ParserConfig inputFormatterConfig);

    [[nodiscard]] MemoryLayoutType getInputMemoryLayoutType() const;

    [[nodiscard]] MemoryLayoutType getOutputMemoryLayoutType() const;

    [[nodiscard]] bool operator==(const MemorySwapLogicalOperator& rhs) const;

    void serialize(SerializableOperator&) const;

    [[nodiscard]] MemorySwapLogicalOperator withTraitSet(TraitSet traitSet) const;

    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] MemorySwapLogicalOperator withChildren(std::vector<LogicalOperator> children) const;

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;

    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] ParserConfig getInputFormatterConfig() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;

    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] MemorySwapLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<EnumWrapper, MemoryLayoutType> INPUT_MEMORY_LAYOUT_TYPE{
            "inputMemoryLayoutType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(INPUT_MEMORY_LAYOUT_TYPE, config); }};
        static inline const DescriptorConfig::ConfigParameter<EnumWrapper, MemoryLayoutType> OUTPUT_MEMORY_LAYOUT_TYPE{
            "outputMemoryLayoutType",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(OUTPUT_MEMORY_LAYOUT_TYPE, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(INPUT_MEMORY_LAYOUT_TYPE, OUTPUT_MEMORY_LAYOUT_TYPE);
    };

private:
    static constexpr std::string_view NAME = "MemorySwap";

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema; ///currently the same schema
    MemoryLayoutType inputMemoryLayoutType, outputMemoryLayoutType;
    ParserConfig inputFormatterConfig;
};

static_assert(LogicalOperatorConcept<MemorySwapLogicalOperator>);
}
