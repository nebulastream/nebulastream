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

#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Report.hpp>

/// Parse + bind the test's SQL snippet into a single physical SourceDescriptor
/// or a single SinkDescriptor on a local catalog. On failure, both populate
/// `report.error` and return nullopt. The two share the parse prologue
/// (`parseProgram`, TU-local in Binder.cpp).

namespace NES::ConnTest
{

std::optional<SourceDescriptor> bindSourceStatements(const std::string& sql, Report& report);
std::optional<SinkDescriptor> bindSinkStatements(const std::string& sql, Report& report);

}
