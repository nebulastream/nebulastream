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

#include <array>
#include <cstdint>
#include <functional>
#include <ostream>
#include <random>
#include <string_view>
#include <variant>

namespace NES::Sources::GeneratorFields
{

static constexpr std::string_view SEQUENCE_IDENTIFIER = "SEQUENCE";
static constexpr std::string_view NORMAL_DISTRIBUTION_IDENTIFIER = "NORMAL_DISTRIBUTION";

/// @brief Variant containing the types that a field can generate
using FieldType = std::variant<uint64_t, int64_t, float, double>;

/// @brief Base class for all types of fields for the generator
class BaseGeneratorField
{
public:
    virtual ~BaseGeneratorField() = default;
    virtual std::ostream& generate(std::ostream& os, std::default_random_engine& /*randEng*/) = 0;
};

class BaseStoppableGeneratorField : public BaseGeneratorField
{
public:
    bool stop{false};
};

constexpr auto NUM_PARAMETERS_SEQUENCE_FIELD = 5;
/// @brief generates sequential records based on the sequence start, end and step size
class SequenceField : public BaseStoppableGeneratorField
{
public:
    SequenceField(FieldType start, FieldType end, FieldType step);
    explicit SequenceField(std::string_view rawSchemaLine);

    std::ostream& generate(std::ostream& os, std::default_random_engine& randEng) override;

    static void validate(std::string_view rawSchemaLine);

    FieldType sequencePosition;
    FieldType sequenceStart;
    FieldType sequenceEnd;
    FieldType sequenceStepSize;

private:
    template <class T>
    void parse(std::string_view start, std::string_view end, std::string_view step);
};

constexpr auto NUM_PARAMETERS_NORMAL_DISTRIBUTION_FIELD = 4;
/// @brief generates normally distríbuted floating point records
class NormalDistributionField final : public BaseGeneratorField
{
public:
    NormalDistributionField(double mean, double stddev);
    explicit NormalDistributionField(std::string_view rawSchemaLine);
    std::ostream& generate(std::ostream& os, std::default_random_engine& randEng) override;
    static void validate(std::string_view rawSchemaLine);

private:
    std::normal_distribution<double> distribution;
};

/// @brief Variant containing the types of base generator fields
using GeneratorFieldType = std::variant<SequenceField, NormalDistributionField>;

struct FieldValidator
{
    std::string_view identifier;
    std::function<void(std::string_view)> validator; /// Validator function throws an Exception if field is invalid
};
/// @brief Array containing functions paired with the fields identifier used to validate the fields syntax
static const std::array<FieldValidator, 2> Validators
    = {{{.identifier = SEQUENCE_IDENTIFIER, .validator = SequenceField::validate},
        {.identifier = NORMAL_DISTRIBUTION_IDENTIFIER, .validator = NormalDistributionField::validate}}};


}
