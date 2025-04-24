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
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources::GeneratorFields
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
            NES_FATAL_ERROR("Could not parse {} as SequenceField {}!", parameter, name);
            throw NES::InvalidConfigParameter("Could not parse {} as SequenceField {}!", parameter, name);
        }
    };
    auto view = rawSchemaLine | std::ranges::views::split(' ')
        | std::views::transform([](const auto& subView) { return std::string_view(subView); });
    const std::vector parameters(view.begin(), view.end());

    if (parameters.size() != NUM_PARAMETERS_SEQUENCE_FIELD)
    {
        NES_FATAL_ERROR("Number of SequenceField parameters does not match! {}", rawSchemaLine);
        throw NES::InvalidConfigParameter("Number of SequenceField parameters does not match! {}", rawSchemaLine);
    }
    const auto type = parameters[1];
    const auto start = parameters[2];
    const auto end = parameters[3];
    const auto step = parameters[4];

    if (type != "FLOAT64" && type != "FLOAT32" && type != "INT64" && type != "UINT64")
    {
        NES_FATAL_ERROR("Invalid SequenceField type of {}!", type);
        throw NES::InvalidConfigParameter("Invalid SequenceField type of {}!", type);
    }
    if (type == "FLOAT64")
    {
        validateParameter.operator()<double>(start, "start");
        validateParameter.operator()<double>(end, "end");
        validateParameter.operator()<double>(step, "step");
    }
    else if (type == "FLOAT32")
    {
        validateParameter.operator()<float>(start, "start");
        validateParameter.operator()<float>(end, "end");
        validateParameter.operator()<float>(step, "step");
    }
    else if (type == "INT64")
    {
        validateParameter.operator()<int64_t>(start, "start");
        validateParameter.operator()<int64_t>(end, "end");
        validateParameter.operator()<int64_t>(step, "step");
    }
    else if (type == "UINT64")
    {
        validateParameter.operator()<uint64_t>(start, "start");
        validateParameter.operator()<uint64_t>(end, "end");
        validateParameter.operator()<uint64_t>(step, "step");
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
    auto view = rawSchemaLine | std::ranges::views::split(' ')
        | std::views::transform([](const auto& subView) { return std::string_view(subView); });
    const std::vector parameters(view.begin(), view.end());

    const auto type = parameters[1];
    const auto start = parameters[2];
    const auto end = parameters[3];
    const auto step = parameters[4];

    if (type == "FLOAT64")
    {
        parse<double>(start, end, step);
    }
    else if (type == "FLOAT32")
    {
        parse<float>(start, end, step);
    }
    else if (type == "INT64")
    {
        parse<int64_t>(start, end, step);
    }
    else if (type == "UINT64")
    {
        parse<uint64_t>(start, end, step);
    }
    else
    {
        throw NES::InvalidConfigParameter("Unknown Type \"{}\" in: {}", type, rawSchemaLine);
    }
    this->stop = false;
}

std::ostream& SequenceField::generate(std::ostream& os, std::default_random_engine& /*re*/)
{
    std::visit(
        [&]<typename T>(T& pos)
        {
            if (this->sequencePosition >= this->sequenceEnd)
            {
                os << pos;
            }
            else
            {
                const auto& step = std::get<T>(sequenceStepSize);
                os << pos;
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

NormalDistributionField::NormalDistributionField(const double mean, const double stddev)
{
    this->distribution = std::normal_distribution(mean, stddev);
}

NormalDistributionField::NormalDistributionField(const std::string_view rawSchemaLine)
{
    auto view = rawSchemaLine | std::ranges::views::split(' ')
        | std::views::transform([](const auto& subView) { return std::string_view(subView); });
    const std::vector parameters(view.begin(), view.end());

    const auto mean = parameters[2];
    const auto stddev = parameters[3];

    const auto parsedMean = Util::from_chars<double>(mean);
    const auto parsedStdDev = Util::from_chars<double>(stddev);

    this->distribution = std::normal_distribution<double>(*parsedMean, *parsedStdDev);
}

std::ostream& NormalDistributionField::generate(std::ostream& os, std::default_random_engine& randEng)
{
    os << std::fixed << std::setprecision(2) << this->distribution(randEng);
    return os;
}

void NormalDistributionField::validate(std::string_view rawSchemaLine)
{
    auto view = rawSchemaLine | std::ranges::views::split(' ')
        | std::views::transform([](const auto& subView) { return std::string_view(subView); });
    const std::vector parameters(view.begin(), view.end());
    if (parameters.size() < NUM_PARAMETERS_NORMAL_DISTRIBUTION_FIELD)
    {
        NES_FATAL_ERROR("Invalid NORMAL_DISTRIBUTION schema line: {}", rawSchemaLine);
        throw NES::InvalidConfigParameter("Invalid NORMAL_DISTRIBUTION schema line: {}", rawSchemaLine);
    }

    const auto type = parameters[1];
    const auto mean = parameters[2];
    const auto stddev = parameters[3];

    if (type != "FLOAT64" || type == "FLOAT32")
    {
        NES_FATAL_ERROR("Invalid Type in NORMAL_DISTRIBUTION, supported are only FLOAT32 or FLOAT64: {}", rawSchemaLine);
        throw NES::InvalidConfigParameter("Invalid Type in NORMAL_DISTRIBUTION, supported are only FLOAT32 or FLOAT64: {}", rawSchemaLine);
    }
    const auto parsedMean = Util::from_chars<double>(mean);
    const auto parsedStdDev = Util::from_chars<double>(stddev);
    if (!parsedMean || !parsedStdDev)
    {
        NES_FATAL_ERROR("Can not parse mean or stddev in {}", rawSchemaLine);
        throw NES::InvalidConfigParameter("Can not parse mean or stddev in {}", rawSchemaLine);
    }
    if (parsedStdDev < 0.0)
    {
        NES_FATAL_ERROR("Stddev must be non-negative");
        throw NES::InvalidConfigParameter("Stddev must be non-negative");
    }
}


}
