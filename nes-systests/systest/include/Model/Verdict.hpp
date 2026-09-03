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

namespace NES
{

/// Why a check failed, as text describing how the actual output differs from the expected one.
/// A struct, so it can later store more structured mismatch information without touching call sites.
struct Mismatch
{
    std::string detail;
};

struct Success
{
};

/// The outcome of one check.
using Verdict = std::expected<Success, Mismatch>;

}
