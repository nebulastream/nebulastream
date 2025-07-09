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
#include <Util/Common.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <ios>
#include <istream>
#include <numeric>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{
}

namespace NES::Util
{


std::filesystem::path createTempDir(std::string_view prefix)
{
    std::string tempDirTemplate(prefix.size() + 6, 'X');
    tempDirTemplate.replace(0, prefix.size(), prefix);
    if (!::mkdtemp(tempDirTemplate.data()))
    {
        throw UnknownException("Could not create temporary directory with prefix {}. Error: {}", prefix, ::strerror(errno));
    }
    return std::filesystem::path(std::move(tempDirTemplate));
}
void TempDirectoryCleanup::cleanup() noexcept
{
    if (!deleteOnExit)
    {
        return;
    }
    try
    {
        if (std::filesystem::exists(*deleteOnExit))
        {
            std::filesystem::remove_all(*deleteOnExit);
        }
        deleteOnExit.reset();
    }
    catch (...)
    {
        tryLogCurrentException();
    }
}

TempDirectoryCleanup::TempDirectoryCleanup(std::filesystem::path delete_on_exit) : deleteOnExit(std::move(delete_on_exit))
{
}

TempDirectoryCleanup::~TempDirectoryCleanup()
{
    cleanup();
}

TempDirectoryCleanup::TempDirectoryCleanup(TempDirectoryCleanup&& other) noexcept
{
    deleteOnExit = std::move(other.deleteOnExit);
    other.deleteOnExit.reset();
}

TempDirectoryCleanup& TempDirectoryCleanup::operator=(TempDirectoryCleanup&& other) noexcept
{
    cleanup();
    deleteOnExit = std::move(other.deleteOnExit);
    other.deleteOnExit.reset();
    return *this;
}
std::string errnoString(int error)
{
    char localBuffer[128] = {};
    std::unique_ptr<char[]> allocation;

    char* realBuffer = localBuffer;
    char realBufferSize = sizeof(localBuffer);

    strerror_l(errno, uselocale(0));

    while (true)
    {
        const auto result = ::strerror_r(error, realBuffer, realBufferSize);
        if (!result)
        {
            if (errno == ERANGE)
            {
                allocation = std::make_unique<char[]>(realBufferSize * 2);
                realBufferSize = realBufferSize * 2;
                realBuffer = allocation.get();
                continue;
            }
            INVARIANT(errno == EINVAL, "The code expects only ERANGE and EINVAL can be returned from strerror_r");
            throw UnknownException("Unknown error code: {}", error);
        }

        return result;
    }
}

}
