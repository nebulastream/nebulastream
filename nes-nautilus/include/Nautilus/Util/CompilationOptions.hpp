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
#include <string>
namespace NES::Nautilus
{

struct CompilationOptions
{
    std::string identifier;
    std::string dumpOutputPath;
    std::string proxyInliningInputPath;
    std::string dumpOutputFileName;
    bool dumpToFile = false;
    bool dumpToConsole = false;
    bool optimize = false;
    bool debug = true;
    bool proxyInlining = false;
    bool cuda = false;
    uint8_t optimizationLevel = 1;
};
} /// namespace NES::Nautilus
