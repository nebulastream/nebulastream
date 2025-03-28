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
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/BinaryLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{
class SerializableOperator;

class JoinLogicalOperator : public BinaryLogicalOperator, public OriginIdAssignmentOperator
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    static constexpr std::string_view NAME = "Join";

    explicit JoinLogicalOperator(
        const std::shared_ptr<LogicalFunction>& joinFunction,
        const std::shared_ptr<Windowing::WindowType>& windowType,
        uint64_t numberOfInputEdgesLeft,
        uint64_t numberOfInputEdgesRight,
        JoinType joinType,
        OriginId originId = INVALID_ORIGIN_ID);
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    bool inferSchema() override;
    std::shared_ptr<Operator> clone() const override;
    [[nodiscard]] bool operator==(const Operator& rhs) const override;

    std::vector<OriginId> getOutputOriginIds() const override;
    void setOriginId(OriginId originId) override;

    [[nodiscard]] std::shared_ptr<LogicalFunction> getJoinFunction() const;
    [[nodiscard]] std::shared_ptr<Schema> getLeftSchema() const;

    [[nodiscard]] std::shared_ptr<Schema> getRightSchema() const;

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    [[nodiscard]] JoinType getJoinType() const;

    void updateSchemas(std::shared_ptr<Schema> leftSourceSchema, std::shared_ptr<Schema> rightSourceSchema);

    [[nodiscard]] std::shared_ptr<Schema> getOutputSchema() const override;

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;

    [[nodiscard]] OriginId getOriginId() const;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, JoinType> JOIN_TYPE{
            "joinType", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return Configurations::DescriptorConfig::tryGet(JOIN_TYPE, config);
            }};

        static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_START_FIELD_NAME{
            "windowStartFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return Configurations::DescriptorConfig::tryGet(WINDOW_START_FIELD_NAME, config);
            }};

        static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> WINDOW_END_FIELD_NAME{
            "windowEndFieldName", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return Configurations::DescriptorConfig::tryGet(WINDOW_END_FIELD_NAME, config);
            }};

        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap(
                JOIN_TYPE, WINDOW_START_FIELD_NAME, WINDOW_END_FIELD_NAME);
    };

    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<LogicalFunction> joinFunction;
    std::shared_ptr<Schema> leftSourceSchema = Schema::create();
    std::shared_ptr<Schema> rightSourceSchema = Schema::create();
    std::shared_ptr<Schema> outputSchema = Schema::create();
    std::shared_ptr<Windowing::WindowType> windowType;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
    JoinType joinType;
};
}
