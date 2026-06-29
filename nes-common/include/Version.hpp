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

#include <iosfwd>
#include <string>
#include <string_view>

namespace NES
{

/// Builds the multi-line build-info report for the given binary name. The first line is
/// `<binaryName> <version>`, followed by indented fields (commit, build, sanitizer, compiler,
/// stdlib, vcpkg baseline). The build metadata is embedded at configure time (see BuildInfo.hpp).
[[nodiscard]] std::string formatVersion(std::string_view binaryName);

/// Prints the build-info report produced by formatVersion to the given stream (defaults to std::cout).
void printVersion(std::string_view binaryName, std::ostream& out);
void printVersion(std::string_view binaryName);

/// Returns true if argv contains a `-v` or `--version` flag. Intended to be checked early in main()
/// so that `<binary> --version` prints the build info via printVersion and exits before any other work.
[[nodiscard]] bool hasVersionFlag(int argc, const char* const* argv);

}
