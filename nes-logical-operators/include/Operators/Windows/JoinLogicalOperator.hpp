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

#include <cstdint>
#include <memory>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Windows/WindowOperator.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Traits/OriginIdTrait.hpp>

namespace NES
{
class SerializableOperator;

class JoinLogicalOperator final : public LogicalOperatorConcept
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinLogicalOperator(LogicalFunction joinFunction,
                                 std::shared_ptr<Windowing::WindowType> windowType,
                                 uint64_t numberOfInputEdgesLeft,
                                 uint64_t numberOfInputEdgesRight,
                                 JoinType joinType);
    std::string_view getName() const noexcept override;

    bool inferSchema();
    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] Schema getLeftSchema() const;
    [[nodiscard]] Schema getRightSchema() const;
    [[nodiscard]] Windowing::WindowType& getWindowType() const;
    [[nodiscard]] JoinType getJoinType() const;

    void updateSchemas(Schema leftSourceSchema, Schema rightSourceSchema);

    [[nodiscard]] Schema getOutputSchema() const override;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;


    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, JoinType>
            JOIN_TYPE{"joinType", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                             return Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config);
                         }};

        static inline const Configurations::DescriptorConfig::ConfigParameter<std::string>
            WINDOW_START_FIELD_NAME{"windowStartFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                          return Configurations::DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config);
                      }};

        static inline const Configurations::DescriptorConfig::ConfigParameter<std::string>
            WINDOW_END_FIELD_NAME{"windowEndFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                                        return Configurations::DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config);
                                    }};

        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap(JOIN_TYPE, WINDOW_START_FIELD_NAME, WINDOW_END_FIELD_NAME);
    };

    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] std::string toString() const override;

    Optimizer::OriginIdTrait originIdTrait;

    std::vector<LogicalOperator> getChildren() const override
    {
        return children;
    }
    void setChildren(std::vector<LogicalOperator> children) override
    {
        this->children = children;
    }

    Optimizer::TraitSet getTraitSet() const override
    {
        return {};
    }

    std::vector<std::vector<OriginId>> getInputOriginIds() const override { return {}; }
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

private:
    static constexpr std::string_view NAME = "Join";

    LogicalFunction joinFunction;
    Schema leftSourceSchema, rightSourceSchema, outputSchema;
    std::shared_ptr<Windowing::WindowType> windowType;
    uint64_t numberOfInputEdgesLeft, numberOfInputEdgesRight;
    std::string windowStartFieldName, windowEndFieldName;
    JoinType joinType;

    std::vector<LogicalOperator> children;
};
}
