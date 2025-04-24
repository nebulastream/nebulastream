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

#include <Generator.hpp>

#include <memory>
#include <ostream>
#include <ranges>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>
#include <GeneratorFields.hpp>

namespace NES::Sources
{

void Generator::generateTuple(std::ostream& ostream)
{
    PRECONDITION(not this->fields.empty(), "Cannot generate a row if there are no fields!");
    const auto generateField = Overloaded{
        [this, &ostream](GeneratorFields::BaseStoppableGeneratorField& field)
        {
            const bool fieldAlreadyStopped = field.stop;
            field.generate(ostream, this->randEng);
            if (field.stop && !fieldAlreadyStopped)
            {
                this->numStoppedFields++;
            }
        },
        [this, &ostream](GeneratorFields::BaseGeneratorField& field) { field.generate(ostream, this->randEng); }};

    std::visit(generateField, *this->fields.front());
    for (auto& field : this->fields | std::views::drop(1))
    {
        ostream << NES::Sources::Generator::fieldDelimiter;
        std::visit(generateField, *field);
    }
    ostream << NES::Sources::Generator::tupleDelimiter;
}

void Generator::addField(std::unique_ptr<GeneratorFields::GeneratorFieldType> field)
{
    std::visit(
        [this]<typename T>(T&)
        {
            if constexpr (std::is_base_of_v<GeneratorFields::BaseStoppableGeneratorField, T>)
            {
                this->numStoppableFields++;
            }
        },
        *field);
    this->numFields++;
    this->fields.emplace_back(std::move(field));
}
/// TODO #355: Parse from YAML Nodes instead of a string
void Generator::parseRawSchemaLine(std::string_view line)
{
    PRECONDITION(!line.empty(), "Line to parse cannot be empty!");
    std::string_view firstWord = line.substr(0, line.find_first_of(' '));
    if (firstWord == GeneratorFields::SEQUENCE_IDENTIFIER)
    {
        this->addField(std::make_unique<GeneratorFields::GeneratorFieldType>(GeneratorFields::SequenceField(line)));
    }
    else if (firstWord == GeneratorFields::NORMAL_DISTRIBUTION_IDENTIFIER)
    {
        this->addField(std::make_unique<GeneratorFields::GeneratorFieldType>(GeneratorFields::NormalDistributionField(line)));
    }
    else
    {
        NES_FATAL_ERROR("Invalid line, {} is not a recognized generatorType: {}", firstWord, line);
        throw NES::InvalidConfigParameter("Invalid line, {} is not a recognized generatorType: {}", firstWord, line);
    }
}

/// TODO #355: Parse from YAML Nodes instead of a string
void Generator::parseSchema(const std::string_view rawSchema)
{
    PRECONDITION(!rawSchema.empty(), "Cannot parse a schema from an empty string!");
    auto view = rawSchema | std::ranges::views::split('\n')
        | std::views::transform([](const auto& subView) { return std::string_view(subView); })
        | std::views::filter([](const auto& subView) { return !subView.empty(); });
    for (const std::vector lines(view.begin(), view.end()); const auto line : lines)
    {
        parseRawSchemaLine(line);
    }
}

bool Generator::shouldStop() const
{
    if (this->sequenceStopsGenerator == GeneratorStop::ALL)
    {
        return this->numStoppableFields == this->numStoppedFields;
    }
    if (this->sequenceStopsGenerator == GeneratorStop::ONE)
    {
        return this->numStoppedFields >= 1;
    }
    return false;
}

}
