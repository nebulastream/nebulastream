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

#include <map>
#include <string>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

/// Source is the interface for all sources that read data into TupleBuffers.
/// 'SourceData' creates TupleBuffers and uses 'Source' to fill.
/// When 'fillTupleBuffer()' returns successfully, 'SourceData' creates a new Task using the filled TupleBuffer.
class Source
{
public:
    Source() = default;
    virtual ~Source() = default;

    /// Read data from a source into a TupleBuffer, until the TupleBuffer is full (or a timeout is reached).
    virtual bool fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer,
        /// Todo #72 : get rid of bufferManager, as soon as parser/formatter is moved out of the Source
        /// passing schema by value to create a new TestTupleBuffer in the Parser.
        NES::Memory::AbstractBufferProvider& bufferManager,
        std::shared_ptr<Schema> schema)
        = 0;

    /// If applicable, opens a connection, e.g., a socket connection to get ready for data consumption.
    virtual void open() = 0;
    /// If applicable, closes a connection, e.g., a socket connection.
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const Source& source);

protected:
    /// Implemented by children of Source. Called by '<<'. Allows to use '<<' on abstract Source.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;

    /// Make sure that SourceSpecificConfiguration::parameterMap exists.
    template <typename T, typename = void>
    struct has_parameter_map : std::false_type
    {
    };
    template <typename T>
    struct has_parameter_map<T, std::void_t<decltype(std::declval<T>().parameterMap)>> : std::true_type
    {
    };
    /// Iterates over all parameters in a user provided config and checks if they are supported by a source-specific config.
    /// Then iterates over all supported config parameters, validates and formats the strings provided by the user.
    /// Uses default parameters if the user did not specify a parameter.
    /// @throws If a mandatory parameter was not provided, an optional parameter was invalid, or a not-supported parameter was encountered.
    template <typename SourceSpecificConfiguration>
    static SourceDescriptor::Config
    validateAndFormatImpl(std::unordered_map<std::string, std::string>&& config, const std::string_view sourceName)
    {
        SourceDescriptor::Config validatedConfig;

        /// First check if all user-specified keys are valid.
        for (const auto& [key, _] : config)
        {
            static_assert(
                has_parameter_map<SourceSpecificConfiguration>::value,
                "A source configuration must have a parameterMap containing all configuration parameters.");
            if (key != "type" && not SourceSpecificConfiguration::parameterMap.contains(key))
            {
                throw(InvalidConfigParameter(fmt::format("Unknown configuration parameter: {}.", key)));
            }
        }
        /// Next, try to validate all config parameters.
        for (const auto& [key, configParameter] : SourceSpecificConfiguration::parameterMap)
        {
            const auto validatedParameter = configParameter.validate(config);
            if (validatedParameter.has_value())
            {
                validatedConfig.emplace(key, validatedParameter.value());
                continue;
            }
            /// If the user did not specify a parameter that is optional, use the default value.
            if (not config.contains(key) && configParameter.getDefaultValue().has_value())
            {
                validatedConfig.emplace(key, configParameter.getDefaultValue().value());
                continue;
            }
            throw InvalidConfigParameter(fmt::format("Failed validation of config parameter: {}, in Source: {}", key, sourceName));
        }
        return validatedConfig;
    }
};

}

/// Specializing the fmt ostream_formatter to accept Source objects.
/// Allows to call fmt::format("Source: {}", sourceObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::Source> : ostream_formatter
{
};
}
