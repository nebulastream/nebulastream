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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <random>
#include <string_view>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>
#include <GeneratorFields.hpp>

namespace NES::Sources
{

enum class GeneratorStop : uint8_t
{
    ALL = 0,
    ONE = 1,
};

/// @brief The Generator encapsulates the functionality of generating rows for the GeneratorSource based on the fields
class Generator
{
public:
    explicit Generator(const uint64_t seed, GeneratorStop sequenceStopsGenerator, const std::string_view rawSchema)
        : sequenceStopsGenerator(std::move(sequenceStopsGenerator)), randEng(std::default_random_engine(seed))
    {
        if (this->sequenceStopsGenerator != GeneratorStop::ALL && this->sequenceStopsGenerator != GeneratorStop::ONE)
        {
            throw NES::InvalidConfigParameter(
                "sequenceStopsGenerator: {} not recognized", static_cast<uint32_t>(this->sequenceStopsGenerator));
        }
        this->parseSchema(rawSchema);
    }

    /// Generates a single row and outputs it into the output stream
    /// @param ostream output stream
    void generateTuple(std::ostream& ostream);

    void addField(std::unique_ptr<GeneratorFields::GeneratorFieldType> field);

    /// TODO #355: Parse from YAML Nodes instead of a string
    /// Parses the generator schema from the @param rawSchema string
    void parseSchema(std::string_view rawSchema);

    [[nodiscard]] bool shouldStop() const;


private:
    static constexpr std::string_view tupleDelimiter = "\n";
    static constexpr std::string_view fieldDelimiter = ",";
    GeneratorStop sequenceStopsGenerator;
    std::vector<std::unique_ptr<GeneratorFields::GeneratorFieldType>> fields;
    std::default_random_engine randEng;

    size_t numFields{0};
    size_t numStoppedFields{0};
    size_t numStoppableFields{0};

    /// TODO #355: Parse from YAML Nodes instead of a string
    void parseRawSchemaLine(std::string_view line);
};
}
