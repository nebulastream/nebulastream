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

#include <Util/Files.hpp>

#include <array>
#include <cerrno>
#include <cstdlib> ///NOLINT(misc-include-cleaner)
#include <cstring> ///NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <unistd.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
std::string getErrorMessageFromERRNO()
{
    return getErrorMessage(errno);
}

std::string getErrorMessage(int errorNumber)
{
    std::array<char, 1024> backupBuffer{};
    const char* errorMessage = strerror_r(errorNumber, backupBuffer.data(), backupBuffer.size());
    INVARIANT(errorMessage != nullptr, "strerror_r does not behave as expected");
    return errorMessage;
}
}
