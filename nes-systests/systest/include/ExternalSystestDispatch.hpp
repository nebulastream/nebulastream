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

#include <filesystem>
#include <string>
#include <vector>

namespace NES::Systest
{

/// Outcome of an attempt to dispatch a `# requires:`-bearing test to the
/// external_systest bats runner.
struct DispatchOutcome
{
    /// True when dispatch happened: the bats runner already executed and
    /// `exitCode` is its exit status. The caller should propagate this code
    /// and not continue with the normal systest run path.
    bool dispatched = false;
    int exitCode = 0;
    /// Human-readable reason dispatch was skipped (only set when
    /// `dispatched == false`). Used by callers to render an error.
    std::string skipReason;
};

/// Run the external_systest bats wrapper for a single .test file that
/// declares one or more `# requires:` profiles. Locates the profile dir
/// (convention: `<plugin>/systest-profile` next to the file's `systests/`
/// directory, or a `# profile-dir:` directive in the test file header),
/// builds the env contract the bats runner expects, and shells out.
///
/// Returns a DispatchOutcome instead of exiting so the caller can decide
/// whether to fall back to the legacy "refuse with error" path when
/// dispatch is impossible (e.g., the profile directory is missing).
DispatchOutcome dispatchExternalSystest(
    const std::filesystem::path& testFile, const std::vector<std::string>& requirements);

}
