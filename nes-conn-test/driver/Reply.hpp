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

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <ErrorHandling.hpp>

/// The JSON reply lines that the harness sends to the runner, one per command.
/// The runner parses them in `conntest_runner/protocol.py`.

namespace NES::ConnTest
{

/// A connector error may be a scenario's expected outcome, a harness error never is.
struct ErrorInfo
{
    std::string origin, phase, name, message;
    std::uint16_t code{0};
};

ErrorInfo connectorErrorInfo(std::string_view phase, const Exception& exception);
ErrorInfo harnessErrorInfo(std::string_view phase, std::string_view name, std::string_view msg);

/// For example `{"ok":false,"phase":"open","origin":"connector","code":4002,"name":"CannotOpenSource","message":"..."}`.
std::string errorReply(const ErrorInfo& err);

/// `{"ok":true,"phase":"open"}`.
std::string okReply(std::string_view phase);
/// `{"ok":true,"phase":"fill","eos":false,"partial_read_ms":[2001]}`.
/// `partial_read_ms` lists how long each read that returned a partly filled buffer took during the step.
std::string fillReply(std::string_view phase, bool eos, const std::vector<std::chrono::milliseconds>& partialReads);

/// The error code's name, as in `ExceptionDefinitions.inc`.
std::string_view errorCodeName(ErrorCode code);

}
