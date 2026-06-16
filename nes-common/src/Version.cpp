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

#include <Version.hpp>

#include <iostream>
#include <string_view>

namespace NES
{

void printVersion(const std::string_view binaryName)
{
    printVersion(binaryName, std::cout);
}

bool hasVersionFlag(const int argc, const char* const* argv)
{
    if (argv == nullptr)
    {
        return false;
    }
    for (int i = 1; i < argc; ++i)
    {
        if (argv[i] == nullptr)
        {
            continue;
        }
        const std::string_view arg{argv[i]};
        if (arg == "-v" || arg == "--version")
        {
            return true;
        }
    }
    return false;
}

}
