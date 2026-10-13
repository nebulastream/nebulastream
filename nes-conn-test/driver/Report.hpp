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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <ErrorHandling.hpp>

/// Error model + helpers shared by the stepper sessions and the binder.
/// `Report` is the binder's error sink (Bind.cpp populates `error` on a
/// failed bind); the sessions serialize an ErrorInfo into a reply line via
/// `errorReplyLine`.

namespace NES::ConnTest
{

struct ErrorInfo
{
    std::string origin, phase, name, message;
    std::uint16_t code{0};
};

/// The binder's failure sink: bindStatements / bindSinkStatements populate
/// `error` (via captureConnector / captureHarness) and return nullopt.
struct Report
{
    std::optional<ErrorInfo> error;
};

void captureConnector(Report& report, std::string_view phase, const Exception& exception);
void captureHarness(Report& report, std::string_view phase, std::string_view name, std::string_view msg);

/// JSON-escape a string body (no surrounding quotes added).
std::string jsonEscape(std::string_view input);

/// Serialize the stepper protocol's one-line failure reply for `err`:
///   {"ok":false,"phase":"open","origin":"connector","code":4002,"name":"CannotOpenSource","message":"…"}
/// The `false` branch reuses the very ErrorInfo built by
/// connectorErrorInfo / harnessErrorInfo, so command replies carry the same
/// fields the old report.json `error` block did.
std::string errorReplyLine(const ErrorInfo& err);

/// Build an ErrorInfo without touching a Report. Sink mode drains on many threads and records only
/// the first error under its own mutex, so it needs the ErrorInfo value rather than the Report
/// mutation captureConnector/captureHarness perform; those are now thin wrappers over these.
ErrorInfo connectorErrorInfo(std::string_view phase, const Exception& exception);
ErrorInfo harnessErrorInfo(std::string_view phase, std::string_view name, std::string_view msg);

}
