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
#include <unordered_map>
#include <variant>

#include <DataTypes/DataType.hpp>
#include "DataTypes/DataType.hpp"

namespace NES::GeneratorFields
{

static constexpr std::string_view SEQUENCE_IDENTIFIER = "SEQUENCE";
static constexpr std::string_view NORMAL_DISTRIBUTION_IDENTIFIER = "NORMAL_DISTRIBUTION";
static constexpr std::string_view WORDLIST_IDENTIFIER = "WORDLIST";
static constexpr std::string_view RANDOMSTR_IDENTIFIER = "RANDOMSTR";

/// @brief Variant containing the types that a field can generate
using FieldType = std::variant<uint64_t, uint32_t, uint16_t, uint8_t, int64_t, int32_t, int16_t, int8_t, float, double>;

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
    /// We define a variant that explicitly lists all possible types.
    /// Otherwise, we would need to template this class, resulting in a lot of unnecessary templates.
    using DistributionVariant = std::variant<
        /// Floating-point types (use normal_distribution)
        std::normal_distribution<float>,
        std::normal_distribution<double>,

        /// Integer types (use binomial_distribution)
        std::binomial_distribution<int8_t>,
        std::binomial_distribution<int16_t>,
        std::binomial_distribution<int32_t>,
        std::binomial_distribution<int64_t>,

        std::binomial_distribution<uint8_t>,
        std::binomial_distribution<uint16_t>,
        std::binomial_distribution<uint32_t>,
        std::binomial_distribution<uint64_t>>;


    explicit NormalDistributionField(std::string_view rawSchemaLine);
    std::ostream& generate(std::ostream& os, std::default_random_engine& randEng) override;
    static void validate(std::string_view rawSchemaLine);

private:
    DistributionVariant distribution;
    DataType outputType;
};

constexpr auto NUM_PARAMETERS_WORDLIST_FIELD = 2;

/// @brief Generates varsized text fields
class WordListField final : public BaseGeneratorField
{
public:
    explicit WordListField(std::string_view rawSchemaLine);
    std::ostream& generate(std::ostream& os, std::default_random_engine& randEng) override;
    static void validate(std::string_view rawSchemaLine);

private:
    std::vector<std::string> wordList;
    std::uniform_int_distribution<size_t> distribution;
};

constexpr auto NUM_PARAMETERS_RANDOMSTR_FIELD = 3;

class RandomStrField final : public BaseGeneratorField
{
public:
    explicit RandomStrField(std::string_view rawSchemaLine);
    std::ostream& generate(std::ostream& os, std::default_random_engine& randEng) override;
    static void validate(std::string_view rawSchemaLine);

private:
    std::size_t minLength;
    std::size_t maxLength;

    std::uniform_int_distribution<uint8_t> distributionCharacter{32, 126}; /// printable ASCII
    std::uniform_int_distribution<size_t> distributionSize;
};

/// @brief Variant containing the types of base generator fields
using GeneratorFieldType = std::variant<SequenceField, NormalDistributionField, WordListField, RandomStrField>;

struct FieldValidator
{
    std::string_view identifier;
    std::function<void(std::string_view)> validator; /// Validator function throws an Exception if field is invalid
};

/// @brief Array containing functions paired with the fields identifier used to validate the fields syntax
/// NOLINTBEGIN(cert-err58-cpp): do not warn about static storage duration
static const std::array<FieldValidator, 4> Validators = {
    {{.identifier = SEQUENCE_IDENTIFIER, .validator = SequenceField::validate},
     {.identifier = NORMAL_DISTRIBUTION_IDENTIFIER, .validator = NormalDistributionField::validate},
     {.identifier = WORDLIST_IDENTIFIER, .validator = WordListField::validate},
     {.identifier = RANDOMSTR_IDENTIFIER, .validator = RandomStrField::validate}},
};
/// NOLINTEND(cert-err58-cpp)

/// @brief Multimap containing key-value pairs of the existing generator fields and which types they accept
/// NOLINTBEGIN(cert-err58-cpp): do not warn about static storage duration
static const std::unordered_multimap<std::string_view, DataType::Type> FieldNameToAcceptedTypes
    = {{SEQUENCE_IDENTIFIER, DataType::Type::INT64},
       {SEQUENCE_IDENTIFIER, DataType::Type::INT32},
       {SEQUENCE_IDENTIFIER, DataType::Type::INT16},
       {SEQUENCE_IDENTIFIER, DataType::Type::INT8},
       {SEQUENCE_IDENTIFIER, DataType::Type::UINT64},
       {SEQUENCE_IDENTIFIER, DataType::Type::UINT32},
       {SEQUENCE_IDENTIFIER, DataType::Type::UINT16},
       {SEQUENCE_IDENTIFIER, DataType::Type::UINT8},
       {SEQUENCE_IDENTIFIER, DataType::Type::FLOAT64},
       {SEQUENCE_IDENTIFIER, DataType::Type::FLOAT32},
       {NORMAL_DISTRIBUTION_IDENTIFIER, DataType::Type::FLOAT64},
       {NORMAL_DISTRIBUTION_IDENTIFIER, DataType::Type::FLOAT32},
       {WORDLIST_IDENTIFIER, DataType::Type::VARSIZED},
       {RANDOMSTR_IDENTIFIER, DataType::Type::VARSIZED}};
/// NOLINTEND(cert-err58-cpp)
}
