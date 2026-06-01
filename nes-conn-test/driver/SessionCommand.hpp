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
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

/// Command-line helpers shared by both stepper sessions (SourceSession /
/// SinkSession). Both parse the same `VERB [<n>]` command grammar off the
/// control channel and both slurp their config/input files the same way, so
/// the helpers live here rather than being copied into each session TU.

namespace NES::ConnTest
{

/// Read an entire file into a string (binary mode). Returns nullopt when the
/// file cannot be opened.
std::optional<std::string> slurpFile(const std::filesystem::path& path);

/// The leading, whitespace-free verb of a command line
/// ("VALIDATE", "WRITE", ...). The verb is everything up to the first space.
std::string_view verbOf(std::string_view line);

/// Parse the unsigned argument of a `VERB <n>` command line. Returns nullopt
/// when the argument is missing or is not a plain non-negative integer —
/// trailing garbage (e.g. "5x") is rejected, not silently truncated.
std::optional<std::uint64_t> argOf(std::string_view line);

}
