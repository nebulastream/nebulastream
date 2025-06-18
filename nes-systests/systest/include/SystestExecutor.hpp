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
#include <vector>

#include <argparse/argparse.hpp>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

struct SystestExecutorResult
{
    enum class ReturnType : uint8_t
    {
        FAILED,
        SUCCESS,
    };
    ReturnType returnType;
    std::string outputMessage;
    std::optional<NES::ErrorCode> errorCode = std::nullopt;
};

namespace NES
{

void runEndlessMode(std::vector<Systest::SystestQuery> queries, Configuration::SystestConfiguration& config);
void loadConfig(const argparse::ArgumentParser& program, Configuration::SystestConfiguration& config);
Configuration::SystestConfiguration readConfiguration(int argc, const char** argv);
void createSymlink(const std::filesystem::path& absoluteLogPath, const std::filesystem::path& symlinkPath);
void setupLogging();

SystestExecutorResult executeSystests(Configuration::SystestConfiguration config);

}
