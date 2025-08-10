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

#include <cstdint>
#include <iomanip>
#include <ios>
#include <ostream>
#include <random>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
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
        const auto opt = Util::from_chars<T>(parameter);
        if (!opt)
        {
            throw InvalidConfigParameter("Could not parse {} as SequenceField {}!", parameter, name);
        }
    };
    const auto parameters = Util::splitStringOnMultipleSpaces<std::string>(rawSchemaLine);
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
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP: {
            throw InvalidConfigParameter("Could not parse {} as SequenceField!", type);
        }
    }
}

template <typename T>
void SequenceField::parse(std::string_view start, std::string_view end, std::string_view step)
{
    const auto startOpt = Util::from_chars<T>(start);
    const auto endOpt = Util::from_chars<T>(end);
    const auto stepOpt = Util::from_chars<T>(step);

    this->sequenceStart = *startOpt;
    this->sequenceEnd = *endOpt;
    this->sequenceStepSize = *stepOpt;
    this->sequencePosition = *startOpt;
}

SequenceField::SequenceField(std::string_view rawSchemaLine)
{
    const auto parameters = Util::splitStringOnMultipleSpaces<std::string>(rawSchemaLine);
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
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP: {
            throw InvalidConfigParameter("Unknown Type \"{}\" in: {}", type, rawSchemaLine);
        }
    }
    this->stop = false;
}

std::ostream& SequenceField::generate(std::ostream& os, std::default_random_engine& /*re*/)
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

NormalDistributionField::NormalDistributionField(const std::string_view rawSchemaLine)
{
    const auto parameters = Util::splitStringOnMultipleSpaces<std::string>(rawSchemaLine);
    const auto type = parameters[1];
    const auto mean = parameters[2];
    const auto stddev = parameters[3];

    const auto parsedMean = Util::from_chars<double>(mean);
    const auto parsedStdDev = Util::from_chars<double>(stddev);

    this->distribution = std::normal_distribution<double>(*parsedMean, *parsedStdDev);
    this->outputType.type = magic_enum::enum_cast<NES::DataType::Type>(type).value();
}

std::ostream& NormalDistributionField::generate(std::ostream& os, std::default_random_engine& randEng)
{
    const auto randValue = this->distribution(randEng);
    switch (outputType.type)
    {
        case DataType::Type::UINT8:
            /// Need to cast it to an int32_t, as we would get 'NULL' and not '0'
            os << std::fixed << static_cast<int32_t>(randValue);
            break;
        case DataType::Type::UINT16:
            os << std::fixed << static_cast<uint16_t>(randValue);
            break;
        case DataType::Type::UINT32:
            os << std::fixed << static_cast<uint32_t>(randValue);
            break;
        case DataType::Type::UINT64:
            os << std::fixed << static_cast<uint64_t>(randValue);
            break;
        case DataType::Type::INT8:
            /// Need to cast it to an int32_t, as we would get 'NULL' and not '0'
            os << std::fixed << static_cast<int32_t>(randValue);
            break;
        case DataType::Type::INT16:
            os << std::fixed << static_cast<int16_t>(randValue);
            break;
        case DataType::Type::INT32:
            os << std::fixed << static_cast<int32_t>(randValue);
            break;
        case DataType::Type::INT64:
            os << std::fixed << static_cast<int64_t>(randValue);
            break;
        case DataType::Type::FLOAT32:
            os << std::fixed << std::setprecision(2) << static_cast<float>(randValue);
            break;
        case DataType::Type::FLOAT64:
            os << std::fixed << std::setprecision(2) << randValue;
            break;
        case DataType::Type::BOOLEAN:
            os << static_cast<bool>(randValue);
            break;
        case DataType::Type::CHAR:
            os << static_cast<char>(randValue);
            break;

        /// Getting a var sized from a normal_distribution is possible but we might want to do something different than solely converting
        /// the value to a string
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw NotImplemented();
    }
    return os;
}

void NormalDistributionField::validate(std::string_view rawSchemaLine)
{
    const auto parameters = Util::splitStringOnMultipleSpaces<std::string>(rawSchemaLine);
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
    const auto parsedMean = Util::from_chars<double>(mean);
    const auto parsedStdDev = Util::from_chars<double>(stddev);
    if (!parsedMean || !parsedStdDev)
    {
        throw InvalidConfigParameter("Can not parse mean or stddev in {}", rawSchemaLine);
    }
    if (parsedStdDev < 0.0)
    {
        throw InvalidConfigParameter("Stddev must be non-negative");
    }
}


}
