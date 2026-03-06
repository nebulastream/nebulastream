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
#include <string>
#include <system_error>

namespace NES::Replay
{

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
