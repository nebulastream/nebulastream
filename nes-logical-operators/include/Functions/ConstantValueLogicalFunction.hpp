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
#include <string>
#include <string_view>
#include <Abstract/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <SerializableFunction.pb.h>
#include <Configurations/Descriptor.hpp>

namespace NES
{

/// This function node represents a constant value and a fixed data type.
/// Thus the samp of this function is always fixed.
class ConstantValueLogicalFunction final : public LogicalFunctionConcept
{
public:
    static constexpr std::string_view NAME = "ConstantValue";

    ConstantValueLogicalFunction(std::shared_ptr<DataType> stamp, std::string constantValueAsString);
    ConstantValueLogicalFunction(const ConstantValueLogicalFunction& other);
    ~ConstantValueLogicalFunction() noexcept override = default;

    std::string getConstantValue() const;

    [[nodiscard]] SerializableFunction serialize() const override;

    void inferStamp(const Schema& schema);
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const;

    const DataType& getStamp() const override { return *stamp; };
    void setStamp(std::shared_ptr<DataType> stamp) override { this->stamp = stamp; };
    std::vector<LogicalFunction> getChildren()  const override { throw UnsupportedOperation(); };
    std::string getType() const override { return std::string(NAME); }
    std::string toString() const override;

    struct ConfigParameters
    {
        static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> CONSTANT_VALUE_AS_STRING{
            "constantValueAsString", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                return NES::Configurations::DescriptorConfig::tryGet(CONSTANT_VALUE_AS_STRING, config);
            }};

        static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
            = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(CONSTANT_VALUE_AS_STRING);
    };

private:
    const std::string constantValue;
    std::shared_ptr<DataType> stamp;
};
}
FMT_OSTREAM(NES::ConstantValueLogicalFunction);
