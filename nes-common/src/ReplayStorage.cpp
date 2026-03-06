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

#include <Replay/ReplayStorage.hpp>

#include <filesystem>
#include <limits>
#include <string>
#include <system_error>

namespace NES::Replay
{

namespace
{
template <typename Projection>
size_t accumulateRecordingDirectory(Projection projection)
{
    const auto recordingDirectory = std::filesystem::path(DEFAULT_RECORDING_DIRECTORY);
    std::error_code ec;
    if (!std::filesystem::exists(recordingDirectory, ec) || ec)
    {
        return 0;
    }

    uintmax_t total = 0;
    for (std::filesystem::recursive_directory_iterator iterator(
             recordingDirectory, std::filesystem::directory_options::skip_permission_denied, ec),
         end;
         iterator != end;
         iterator.increment(ec))
    {
        if (ec)
        {
            ec.clear();
            continue;
        }

        total += projection(*iterator, ec);
        if (ec)
        {
            ec.clear();
        }
    }

    return static_cast<size_t>(std::min<uintmax_t>(total, std::numeric_limits<size_t>::max()));
}
}

std::string getRecordingFilePath(std::string_view recordingId)
{
    return (std::filesystem::path(DEFAULT_RECORDING_DIRECTORY) / std::string(recordingId)).concat(".bin").string();
}

std::string getTimeTravelReadAliasPath()
{
    return std::string(DEFAULT_RECORDING_FILE_PATH);
}

std::string resolveTimeTravelReadProbePath()
{
    const auto aliasPath = std::filesystem::path(getTimeTravelReadAliasPath());
    std::error_code ec;
    if (std::filesystem::is_symlink(aliasPath, ec))
    {
        auto target = std::filesystem::read_symlink(aliasPath, ec);
        if (!ec)
        {
            if (target.is_relative())
            {
                return (aliasPath.parent_path() / target).lexically_normal().string();
            }
            return target.string();
        }
    }
    return aliasPath.string();
}

size_t getRecordingStorageBytes()
{
    return accumulateRecordingDirectory(
        [](const std::filesystem::directory_entry& entry, std::error_code& ec) -> uintmax_t
        {
            if (!entry.is_regular_file(ec))
            {
                return 0;
            }
            return entry.file_size(ec);
        });
}

size_t getRecordingFileCount()
{
    return accumulateRecordingDirectory(
        [](const std::filesystem::directory_entry& entry, std::error_code& ec) -> uintmax_t
        {
            if (entry.is_regular_file(ec))
            {
                return 1;
            }
            return 0;
        });
}

void updateTimeTravelReadAlias(const std::string& recordingFilePath)
{
    const auto aliasPath = std::filesystem::path(getTimeTravelReadAliasPath());
    const auto targetPath = std::filesystem::path(recordingFilePath);

    std::error_code ec;
    if (!aliasPath.parent_path().empty())
    {
        std::filesystem::create_directories(aliasPath.parent_path(), ec);
        ec.clear();
    }
    if (!targetPath.parent_path().empty())
    {
        std::filesystem::create_directories(targetPath.parent_path(), ec);
        ec.clear();
    }

    std::filesystem::remove(aliasPath, ec);
    ec.clear();
    std::filesystem::create_symlink(targetPath, aliasPath, ec);
}

void clearTimeTravelReadAlias()
{
    std::error_code ec;
    std::filesystem::remove(std::filesystem::path(getTimeTravelReadAliasPath()), ec);
}

}
