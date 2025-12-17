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

#include <GeneratorFields.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <ios>
#include <ostream>
#include <random>
#include <ranges>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::GeneratorFields
{
SequenceField::SequenceField(const FieldType start, const FieldType end, const FieldType step)
    : sequencePosition(start), sequenceStart(start), sequenceEnd(end), sequenceStepSize(step)
{
}

void SequenceField::validate(std::string_view rawSchemaLine)
{
    auto validateParameter = []<typename T>(std::string_view parameter, std::string_view name)
    {
        const auto opt = from_chars<T>(parameter);
        if (!opt)
        {
            throw InvalidConfigParameter("Could not parse {} as SequenceField {}!", parameter, name);
        }
    };
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    if (parameters.size() != NUM_PARAMETERS_SEQUENCE_FIELD)
    {
        throw InvalidConfigParameter("Number of SequenceField parameters does not match! {}", rawSchemaLine);
    }
    const auto type = parameters[1];
    const auto start = parameters[2];
    const auto end = parameters[3];
    const auto step = parameters[4];

    const auto dataType = DataTypeProvider::tryProvideDataType(std::string{type});
    if (not dataType.has_value())
    {
        throw InvalidConfigParameter("Invalid SequenceField type of {}!", type);
    }
    switch (dataType.value().type)
    {
        case DataType::Type::UINT8: {
            validateParameter.operator()<uint8_t>(start, "start");
            validateParameter.operator()<uint8_t>(end, "end");
            validateParameter.operator()<uint8_t>(step, "step");
            break;
        }
        case DataType::Type::UINT16: {
            validateParameter.operator()<uint16_t>(start, "start");
            validateParameter.operator()<uint16_t>(end, "end");
            validateParameter.operator()<uint16_t>(step, "step");
            break;
        }
        case DataType::Type::UINT32: {
            validateParameter.operator()<uint32_t>(start, "start");
            validateParameter.operator()<uint32_t>(end, "end");
            validateParameter.operator()<uint32_t>(step, "step");
            break;
        }
        case DataType::Type::UINT64: {
            validateParameter.operator()<uint64_t>(start, "start");
            validateParameter.operator()<uint64_t>(end, "end");
            validateParameter.operator()<uint64_t>(step, "step");
            break;
        }
        case DataType::Type::INT8: {
            validateParameter.operator()<int8_t>(start, "start");
            validateParameter.operator()<int8_t>(end, "end");
            validateParameter.operator()<int8_t>(step, "step");
            break;
        }
        case DataType::Type::INT16: {
            validateParameter.operator()<int16_t>(start, "start");
            validateParameter.operator()<int16_t>(end, "end");
            validateParameter.operator()<int16_t>(step, "step");
            break;
        }
        case DataType::Type::INT32: {
            validateParameter.operator()<int32_t>(start, "start");
            validateParameter.operator()<int32_t>(end, "end");
            validateParameter.operator()<int32_t>(step, "step");
            break;
        }
        case DataType::Type::INT64: {
            validateParameter.operator()<int64_t>(start, "start");
            validateParameter.operator()<int64_t>(end, "end");
            validateParameter.operator()<int64_t>(step, "step");
            break;
        }
        case DataType::Type::FLOAT32: {
            validateParameter.operator()<float>(start, "start");
            validateParameter.operator()<float>(end, "end");
            validateParameter.operator()<float>(step, "step");
            break;
        }
        case DataType::Type::FLOAT64: {
            validateParameter.operator()<double>(start, "start");
            validateParameter.operator()<double>(end, "end");
            validateParameter.operator()<double>(step, "step");
            break;
        }
        case DataType::Type::BOOLEAN: {
            validateParameter.operator()<bool>(start, "start");
            validateParameter.operator()<bool>(end, "end");
            validateParameter.operator()<bool>(step, "step");
            break;
        }
        case DataType::Type::CHAR: {
            validateParameter.operator()<char>(start, "start");
            validateParameter.operator()<char>(end, "end");
            validateParameter.operator()<char>(step, "step");
            break;
        }
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED: {
            throw InvalidConfigParameter("Could not parse {} as SequenceField!", type);
        }
    }
}

template <typename T>
void SequenceField::parse(std::string_view start, std::string_view end, std::string_view step)
{
    const auto startOpt = from_chars<T>(start);
    const auto endOpt = from_chars<T>(end);
    const auto stepOpt = from_chars<T>(step);

    this->sequenceStart = *startOpt;
    this->sequenceEnd = *endOpt;
    this->sequenceStepSize = *stepOpt;
    this->sequencePosition = *startOpt;
}

SequenceField::SequenceField(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    const auto type = parameters[1];
    const auto start = parameters[2];
    const auto end = parameters[3];
    const auto step = parameters[4];
    const auto dataType = DataTypeProvider::tryProvideDataType(std::string{type});
    if (not dataType.has_value())
    {
        throw InvalidConfigParameter("Invalid SequenceField type of {}!", type);
    }
    switch (dataType.value().type)
    {
        case DataType::Type::UINT8: {
            parse<uint8_t>(start, end, step);
            break;
        }
        case DataType::Type::UINT16: {
            parse<uint16_t>(start, end, step);
            break;
        }
        case DataType::Type::UINT32: {
            parse<uint32_t>(start, end, step);
            break;
        }
        case DataType::Type::UINT64: {
            parse<uint64_t>(start, end, step);
            break;
        }
        case DataType::Type::INT8: {
            parse<int8_t>(start, end, step);
            break;
        }
        case DataType::Type::INT16: {
            parse<int16_t>(start, end, step);
            break;
        }
        case DataType::Type::INT32: {
            parse<int32_t>(start, end, step);
            break;
        }
        case DataType::Type::INT64: {
            parse<int64_t>(start, end, step);
            break;
        }
        case DataType::Type::FLOAT32: {
            parse<float>(start, end, step);
            break;
        }
        case DataType::Type::FLOAT64: {
            parse<double>(start, end, step);
            break;
        }
        case DataType::Type::BOOLEAN: {
            parse<bool>(start, end, step);
            break;
        }
        case DataType::Type::CHAR: {
            parse<char>(start, end, step);
            break;
        }
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED: {
            INVARIANT(false, "Unknown Type \"{}\" in: {}", type, rawSchemaLine);
        }
    }
    this->stop = false;
}

std::ostream& SequenceField::generate(std::ostream& os, std::mt19937& /*re*/)
{
    std::visit(
        [&]<typename T>(T& pos)
        {
            if constexpr (std::is_same_v<T, uint8_t> or std::is_same_v<T, int8_t>)
            {
                /// Need to cast it to an int32, as we would get 'NULL' and not '0'
                os << static_cast<int32_t>(pos);
            }
            else
            {
                os << pos;
            }

            if (this->sequencePosition < this->sequenceEnd)
            {
                const auto& step = std::get<T>(sequenceStepSize);
                pos += step;
            }
        },
        sequencePosition);
    if (sequencePosition >= this->sequenceEnd)
    {
        this->stop = true;
    }
    return os;
}

namespace
{
template <typename T, typename U = double>
NormalDistributionField::DistributionVariant createDistribution(const std::string_view mean, const std::string_view stdDev)
{
    const auto parsedMean = from_chars<T>(mean);
    const auto parsedStdDev = from_chars<U>(stdDev);
    INVARIANT(parsedMean.has_value(), "Could not parse mean from {}", mean);
    INVARIANT(parsedStdDev.has_value(), "Could not parse std dev from {}", stdDev);

    if constexpr (std::is_same_v<T, double> or std::is_same_v<T, float>)
    {
        return std::normal_distribution<T>(*parsedMean, *parsedStdDev);
    }
    else
    {
        return std::binomial_distribution<T>(*parsedMean, *parsedStdDev);
    }
};
}

NormalDistributionField::NormalDistributionField(const std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    const auto type = parameters[1];
    const auto mean = parameters[2];
    const auto stddev = parameters[3];


    outputType.type = magic_enum::enum_cast<NES::DataType::Type>(type).value();
    switch (outputType.type)
    {
        case DataType::Type::UINT8:
            distribution = createDistribution<uint8_t>(mean, stddev);
            break;
        case DataType::Type::UINT16:
            distribution = createDistribution<uint16_t>(mean, stddev);
            break;
        case DataType::Type::UINT32:
            distribution = createDistribution<uint32_t>(mean, stddev);
            break;
        case DataType::Type::UINT64:
            distribution = createDistribution<uint64_t>(mean, stddev);
            break;
        case DataType::Type::INT8:
            distribution = createDistribution<int8_t>(mean, stddev);
            break;
        case DataType::Type::INT16:
            distribution = createDistribution<int16_t>(mean, stddev);
            break;
        case DataType::Type::INT32:
            distribution = createDistribution<int32_t>(mean, stddev);
            break;
        case DataType::Type::INT64:
            distribution = createDistribution<int64_t>(mean, stddev);
            break;
        case DataType::Type::FLOAT32:
            distribution = createDistribution<float, float>(mean, stddev);
            break;
        case DataType::Type::FLOAT64:
            distribution = createDistribution<double, double>(mean, stddev);
            break;

        /// We require an integer for binomial_distribution
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
            INVARIANT(false, "Output Type \"{}\" is not supported for normal or binomial distribution.", outputType);

        /// Getting a var sized from a normal_distribution is possible but we might want to do something different than solely converting
        /// the value to a string
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED: {
            INVARIANT(false, "Output Type \"{}\" is not supported for normal or binomial distribution.", outputType);
        }
    }
}

std::ostream& NormalDistributionField::generate(std::ostream& os, std::mt19937& randEng)
{
    std::visit(
        [&os, &randEng, copyOfOutputType = outputType.type](auto& distribution)
        {
            if (copyOfOutputType == DataType::Type::UINT8 or copyOfOutputType == DataType::Type::INT8)
            {
                /// Need to cast it to an int32_t, as we would get 'NULL' and not '0'
                os << static_cast<int32_t>(distribution(randEng));
            }
            else
            {
                os << distribution(randEng);
            }
        },
        distribution);
    return os;
}

void NormalDistributionField::validate(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    if (parameters.size() < NUM_PARAMETERS_NORMAL_DISTRIBUTION_FIELD)
    {
        throw InvalidConfigParameter("Invalid NORMAL_DISTRIBUTION schema line: {}", rawSchemaLine);
    }

    const auto typeParam = parameters[1];
    const auto mean = parameters[2];
    const auto stddev = parameters[3];

    if (const auto type = magic_enum::enum_cast<NES::DataType::Type>(typeParam); not type.has_value())
    {
        constexpr auto allDataTypes = magic_enum::enum_names<DataType::Type>();
        NES_ERROR("Invalid Type in NORMAL_DISTRIBUTION, supported are only {} {}", fmt::join(allDataTypes, ","), rawSchemaLine);
        throw InvalidConfigParameter(
            "Invalid Type in NORMAL_DISTRIBUTION, supported are only {}: {}", fmt::join(allDataTypes, ","), rawSchemaLine);
    }
    const auto parsedMean = from_chars<double>(mean);
    const auto parsedStdDev = from_chars<double>(stddev);
    if (!parsedMean || !parsedStdDev)
    {
        throw InvalidConfigParameter("Can not parse mean or stddev in {}", rawSchemaLine);
    }
    if (parsedStdDev < 0.0)
    {
        throw InvalidConfigParameter("Stddev must be non-negative");
    }
}

WordListField::WordListField(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    INVARIANT(
        not parameters.empty(),
        "Invalid WORDLIST schema line: {}! Number of parameters should be {}",
        rawSchemaLine,
        NUM_PARAMETERS_WORDLIST_FIELD);

    const auto path = std::filesystem::path(SYSTEST_DATA_DIR) / std::string(parameters[1]);
    INVARIANT(std::filesystem::exists(path), "Invalid WORDLIST schema line: {}! Filepath {} does not exist!", rawSchemaLine, path);
    std::ifstream wordListFile(std::string(path), std::ios::in);

    std::string line;
    while (std::getline(wordListFile, line))
    {
        if (not line.empty())
        {
            wordList.emplace_back(line);
        }
    }
    wordListFile.close();
}

std::ostream& WordListField::generate(std::ostream& os, std::mt19937& randEng)
{
    const auto randomWordPos = randEng() % wordList.size();
    const auto word = wordList[randomWordPos];
    os << word;

    return os;
}

void WordListField::validate(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    if (parameters.size() != NUM_PARAMETERS_WORDLIST_FIELD)
    {
        throw InvalidConfigParameter("Invalid WORDLIST schema line: {}", rawSchemaLine);
    }

    const auto path = std::filesystem::path(SYSTEST_DATA_DIR) / std::string(parameters[1]);
    if (not std::filesystem::exists(path))
    {
        throw InvalidConfigParameter("Invalid WORDLIST schema! Path {} does not exist! Schema line: {}", path, rawSchemaLine);
    }

    std::ifstream wordListFile(std::string(path), std::ios::in);
    if (not wordListFile.is_open() or wordListFile.fail())
    {
        throw InvalidConfigParameter("Failed to open file containing the word list at {}", path);
    }

    size_t wordCount = 0;
    std::string line;
    while (std::getline(wordListFile, line))
    {
        if (not line.empty())
        {
            wordCount++;
        }
    }

    if (wordCount == 0)
    {
        throw InvalidConfigParameter("Invalid WORDLIST schema! File at {} contains no words!", path);
    }
    wordListFile.close();
}

RandomStrField::RandomStrField(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    if (parameters.size() < NUM_PARAMETERS_RANDOMSTR_FIELD)
    {
        throw InvalidConfigParameter("Invalid RANDOMSTR_FIELD schema line: {}!", rawSchemaLine);
    }

    const auto parsedMinLength = from_chars<size_t>(parameters[1]);
    INVARIANT(
        parsedMinLength.has_value(),
        "Invalid RANDOMSTR_FIELD schema line: {}! Could not parse a minLength from {}",
        rawSchemaLine,
        parameters[1]);
    const auto parsedMaxLength = from_chars<size_t>(parameters[2]);
    INVARIANT(
        parsedMaxLength.has_value(),
        "Invalid RANDOMSTR_FIELD schema line: {}! Could not parse a maxLength from {}",
        rawSchemaLine,
        parameters[2]);
    INVARIANT(
        parsedMinLength >= 0,
        "Invaild RANDOMSTR parameter MINLENGTH: {} <= 0! The MINLENGTH must be larger than 0! Schema line: {}",
        parsedMinLength,
        rawSchemaLine);
    INVARIANT(
        parsedMaxLength >= 0,
        "Invaild RANDOMSTR parameter MAXLENGTH: {} <= 0! The MAXLENGTH must be larger than 0! Schema line: {}",
        parsedMaxLength,
        rawSchemaLine);
    INVARIANT(
        parsedMinLength <= parsedMaxLength,
        "Invalid RANDOMSTR parameters MINLENGTH: {} > MAXLENGTH: {}! The MINLENGTH can not be longer than the MAXLENGTH! Schema line: "
        "{}",
        parsedMinLength,
        parsedMaxLength,
        rawSchemaLine);

    this->minLength = parsedMinLength.value();
    this->maxLength = parsedMaxLength.value();
}

void RandomStrField::validate(std::string_view rawSchemaLine)
{
    const auto parameters = splitWithStringDelimiter<std::string_view>(rawSchemaLine, " ");
    if (parameters.size() != NUM_PARAMETERS_RANDOMSTR_FIELD)
    {
        throw InvalidConfigParameter("Invalid RANDOMSTR schema line: {}", rawSchemaLine);
    }
    const auto minLength = parameters[1];
    const auto maxLength = parameters[2];

    const auto parsedMinLength = from_chars<size_t>(minLength);
    const auto parsedMaxLength = from_chars<size_t>(maxLength);

    if (not parsedMinLength)
    {
        throw InvalidConfigParameter("Invalid RANDOMSTR parameter MINLENGTH! Cannot parse MINLENGTH! Schema line: {}", rawSchemaLine);
    }
    if (not parsedMaxLength)
    {
        throw InvalidConfigParameter("Invalid RANDOMSTR parameter MAXLENGTH! Cannot parse MAXLENGTH! Schema line: {}", rawSchemaLine);
    }

    if (parsedMinLength <= 0)
    {
        throw InvalidConfigParameter(
            "Invaild RANDOMSTR parameter MINLENGTH: {} <= 0! The MINLENGTH must be larger than 0! Schema line: {}",
            parsedMinLength,
            rawSchemaLine);
    }
    if (parsedMaxLength <= 0)
    {
        throw InvalidConfigParameter(
            "Invaild RANDOMSTR parameter MAXLENGTH: {} <= 0! The MAXLENGTH must be larger than 0! Schema line: {}",
            parsedMaxLength,
            rawSchemaLine);
    }

    if (parsedMinLength > parsedMaxLength)
    {
        throw InvalidConfigParameter(
            "Invalid RANDOMSTR parameters MINLENGTH: {} > MAXLENGTH: {}! The MINLENGTH can not be longer than the MAXLENGTH! Schema line: "
            "{}",
            parsedMinLength,
            parsedMaxLength,
            rawSchemaLine);
    }
}

std::ostream& RandomStrField::generate(std::ostream& os, std::mt19937& randEng)
{
    const auto randomLength = [&randEng, this] { return this->minLength + (randEng() % (this->maxLength - this->minLength + 1)); }();
    auto randomAlphabetChar = [&randEng]
    {
        const auto index = randEng() % BASE64_ALPHABET.size();
        INVARIANT(
            index < NES::GeneratorFields::RandomStrField::BASE64_ALPHABET.size(),
            "Index into BASE64_ALPHABET {} cannot exceed this Alphabet's size {}!",
            index,
            NES::GeneratorFields::RandomStrField::BASE64_ALPHABET.size());
        return BASE64_ALPHABET.at(index);
    };

    for (size_t i = 0; i < randomLength; i++)
    {
        os << randomAlphabetChar();
    }
    return os;
}


}
