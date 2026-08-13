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
#include <string>
#include <Configurations/ConfigLiteral.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{
/// Lowest priority first: run arguments/file, topology, then test-file settings.
WorkerConfigResolver makeWorkerConfigResolver(
    Schema<LiteralConfigValue, Ordered> commandLineLiterals,
    Schema<LiteralConfigValue, Ordered> systestFileLiterals,
    bool permitConflictingOptions,
    std::string overwriteContext);
}
