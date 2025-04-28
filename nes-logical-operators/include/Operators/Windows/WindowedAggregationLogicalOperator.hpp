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

#include <memory>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Traits/OriginIdAssignerTrait.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{

class WindowedAggregationLogicalOperator final : public LogicalOperatorConcept
{
public:
    WindowedAggregationLogicalOperator(
        std::vector<FieldAccessLogicalFunction> onKey,
        std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation,
        std::shared_ptr<Windowing::WindowType> windowType);


    /// Operator specific member
    std::vector<std::string> getGroupByKeyNames() const;
    bool isKeyed() const;

    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> getWindowAggregation() const;
    void setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation);

    [[nodiscard]] Windowing::WindowType& getWindowType() const;
    void setWindowType(std::unique_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] std::vector<FieldAccessLogicalFunction> getKeys() const;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;


    /// LogicalOperatorConcept member
    [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override;
    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;

    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override;

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

    /// Serialization
 struct ConfigParameters
 {
     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint64_t> TIME_MS{
         "TimeMs", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(TIME_MS, config);
         }};

     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<AggregationFunctionList> WINDOW_AGGREGATIONS{
         "windowAggregations", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(WINDOW_AGGREGATIONS, config);
         }};

     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<FunctionList> WINDOW_KEYS{
         "windowKeys", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(WINDOW_KEYS, config);
         }};

     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_START_FIELD_NAME{
         "windowStartFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config);
         }};

     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_END_FIELD_NAME{
         "windowEndFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config);
         }};

     static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_INFOS{
         "windowInfos", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
             return NES::Configurations::DescriptorConfig::tryGet(WINDOW_INFOS, config);
         }};

     static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
         = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(
             TIME_MS,
             WINDOW_AGGREGATIONS,
             WINDOW_INFOS,
             WINDOW_KEYS,
             WINDOW_START_FIELD_NAME,
             WINDOW_END_FIELD_NAME
         );
 };

private:
    /// Operator specific member
    static constexpr std::string_view NAME = "WindowedAggregation";
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<FieldAccessLogicalFunction> onKey;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
    Optimizer::OriginIdAssignerTrait originIdTrait;

    /// LogicalOperatorConcept member
    std::vector<LogicalOperator> children;
    std::vector<OriginId> inputOriginIds;
    Schema inputSchema, outputSchema;
    std::vector<OriginId> outputOriginIds;
};

}
