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

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using OutputFormatterRegistryReturnType = std::unique_ptr<OutputFormatter>;

struct OutputFormatterRegistryArguments
{
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    OutputFormatterDescriptor descriptor;
};

using OutputFormatterFactoryFn = std::function<OutputFormatterRegistryReturnType(OutputFormatterRegistryArguments)>;

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Entries are static provideFormatter members on the formatter classes (constructor signatures
/// differ between formatters, so a uniform factory template does not fit).
/// Case-insensitive to mirror the retired BaseRegistry.
class OutputFormatterRegistry
    : public RuntimeRegistry<OutputFormatterRegistry, std::string, OutputFormatterFactoryFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (OutputFormatterRegistry.cpp) so exactly one instance exists
    /// process-wide even with plugins loaded as shared objects.
    static OutputFormatterRegistry& instance();
};

}
