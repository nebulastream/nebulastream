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
#include <Configurations/Descriptor.hpp>
#include <Operators/LogicalOperators/BinaryLogicalOperator.hpp>
#include "BinaryLogicalOperator.hpp"

namespace NES
{

class UnionLogicalOperator : public BinaryLogicalOperator
{
public:
    static constexpr std::string_view NAME = "Union";

    explicit UnionLogicalOperator();
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    void inferInputOrigins() override;
    std::shared_ptr<Operator> clone() const override;
    [[nodiscard]] bool operator==(const Operator& rhs) const override;

    [[nodiscard]] SerializableOperator serialize() const override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    struct ConfigParameters
    {
        static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = Configurations::DescriptorConfig::createConfigParameterContainerMap();
    };

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
};


}
