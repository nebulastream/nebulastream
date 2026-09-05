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

#include <vector>

#include <Model/ConfigurationOverride.hpp>
#include <Model/ParsedTestFile.hpp>

namespace NES
{

/// One configuration override and the statements of a test file that ask for it.
/// Each part runs on its own worker, because a worker takes its configuration at startup and cannot change it later.
struct TestFilePart
{
    ConfigurationOverride overrides;
    ParsedTestFile file;
};

/// Splits a test file into one part per distinct configuration override, in the order the file first asks for each.
/// Every part repeats the file's CREATE statements, because a query reads the sources declared on the worker running it.
/// A file whose queries all ask for the same configuration yields a single part holding the file unchanged.
[[nodiscard]] std::vector<TestFilePart> partitionByOverrides(const ParsedTestFile& testFile);

}
