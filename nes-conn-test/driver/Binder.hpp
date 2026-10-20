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

#include <expected>
#include <string>

#include <Sources/SourceDescriptor.hpp>
#include <Reply.hpp>

namespace NES::ConnTest
{

/// Binds the test's SQL into one physical source descriptor on a local catalog.
/// Every failure is a harness error, because the committed configs are checked to bind and an invalid config is never a scenario.
std::expected<SourceDescriptor, ErrorInfo> bindSourceStatements(const std::string& sql);

}
