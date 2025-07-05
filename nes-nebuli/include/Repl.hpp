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

#include <memory>
#include <string>
#include <vector>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <GRPCClient.hpp>
#include <replxx.hxx>

namespace NES
{

class Repl
{
    std::shared_ptr<GRPCClient> grpcClient;
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;

    std::unique_ptr<replxx::Replxx> rx;
    std::vector<std::string> history;
    bool interactiveMode = true;

    /// Commands
    static constexpr const char* HELP_CMD = "help";
    static constexpr const char* QUIT_CMD = "quit";
    static constexpr const char* EXIT_CMD = "exit";
    static constexpr const char* CLEAR_CMD = "clear";

    void setupReplxx();
    static void printWelcome();
    static void printHelp();
    void clearScreen();

    [[nodiscard]] static std::string getPrompt();
    [[nodiscard]] static bool isCommand(const std::string& input);
    [[nodiscard]] bool handleCommand(const std::string& input);
    [[nodiscard]] bool executeQuery(const std::string& query);
    [[nodiscard]] std::string readMultiLineQuery() const;

public:
    explicit Repl(std::shared_ptr<GRPCClient> client);
    void run();
};

}
