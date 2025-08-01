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
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// The projection operator only narrows down the fields of an input schema to a smaller subset. The map operator handles renaming and adding new fields.
class ProjectionLogicalOperator : public LogicalOperatorHelper<ProjectionLogicalOperator>
{
public:
    class Asterisk
    {
        bool value;

    public:
        explicit Asterisk(bool value) : value(value) { }
        friend ProjectionLogicalOperator;
    };
    using Projection = std::pair<std::optional<FieldIdentifier>, LogicalFunction>;
    ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk);

    [[nodiscard]] const std::vector<Projection>& getProjections() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] std::vector<std::string> getAccessedFields() const;

    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override;

private:
    struct Data
    {
        std::vector<std::pair<std::optional<FieldIdentifier>, LogicalFunction>> projections;
        bool asterisk;
    };

    static constexpr std::string_view NAME = "Projection";
    std::vector<Projection> projections;
    Data data;
};

}
