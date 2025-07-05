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
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <GRPCClient.hpp>
#include <replxx.hxx>

namespace NES::CLI
{

class Repl
{
    std::shared_ptr<GRPCClient> grpcClient;
    std::unique_ptr<replxx::Replxx> rx;
    std::vector<std::string> history;
    std::vector<QueryId> activeQueries;
    bool interactiveMode = true;

    /// Commands
    static constexpr const char* HELP_CMD = "help";
    static constexpr const char* QUIT_CMD = "quit";
    static constexpr const char* EXIT_CMD = "exit";
    static constexpr const char* LIST_CMD = "list";
    static constexpr const char* STOP_CMD = "stop";
    static constexpr const char* CLEAR_CMD = "clear";

    void setupReplxx();
    void printWelcome();
    void printHelp();
    void printActiveQueries();
    void stopQuery(const std::string& input);
    void clearScreen();
    std::string getPrompt() const;
    bool isCommand(const std::string& input) const;
    void handleCommand(const std::string& input);
    bool executeQuery(const std::string& query);
    std::string readMultiLineQuery();
    LogicalPlan parseAndOptimizeQuery(const std::string& query);

public:
    explicit Repl(std::shared_ptr<GRPCClient> client, bool interactive = true);
    ~Repl();

    void run();
};

}
