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

#include <Repl.hpp>

#include <csignal>
#include <iostream>
#include <sstream>
#include <string>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceCatalog.hpp>
#include <YAML/YAMLBinder.hpp>
#include <NebuLI.hpp>

namespace NES::CLI
{

Repl::Repl(std::shared_ptr<GRPCClient> client, bool interactive) : grpcClient(std::move(client)), interactiveMode(interactive)
{
    setupReplxx();
}

void Repl::setupReplxx()
{
    rx = std::make_unique<replxx::Replxx>();

    rx->set_word_break_characters(" \t\n\r");

    /// Set up hints
    rx->set_hint_callback(
        [](const std::string& input, int&, replxx::Replxx::Color& color) -> std::vector<std::string>
        {
            if (input.empty())
            {
                return {};
            }

            std::vector<std::string> commands = {"help", "quit", "exit", "list", "stop", "clear"};
            for (const auto& cmd : commands)
            {
                if (input.find(cmd) == 0)
                {
                    color = replxx::Replxx::Color::BLUE;
                    return {" (command)"};
                }
            }

            return {};
        });

    rx->history_load(".nebuli_history");
}

void Repl::printWelcome()
{
    std::cout << "\n=== NebulaStream Interactive Query Shell ===\n";
    std::cout << "Type 'help' for available commands\n";
    std::cout << "Type SQL queries to execute them\n";
    std::cout << "Type 'quit' or 'exit' to exit\n\n";
}

void Repl::printHelp()
{
    std::cout << "\nAvailable commands:\n";
    std::cout << "  help     - Show this help message\n";
    std::cout << "  list     - List active queries\n";
    std::cout << "  stop <id> - Stop a specific query by ID\n";
    std::cout << "  clear    - Clear the screen\n";
    std::cout << "  quit     - Exit the REPL\n";
    std::cout << "  exit     - Exit the REPL\n\n";
    std::cout << "You can also enter SQL queries directly.\n";
    std::cout << "Multi-line queries are supported.\n\n";
}

void Repl::printActiveQueries()
{
    if (activeQueries.empty())
    {
        std::cout << "No active queries.\n";
        return;
    }

    std::cout << "Active queries:\n";
    for (const auto& queryId : activeQueries)
    {
        std::cout << "  Query ID: " << queryId.getRawValue() << "\n";
    }
}

void Repl::stopQuery(const std::string& input)
{
    std::istringstream iss(input);
    std::string cmd;
    size_t queryId;

    iss >> cmd >> queryId;

    try
    {
        grpcClient->stop(QueryId{queryId});
        std::cout << "Stopped query " << queryId << "\n";

        /// Remove from active queries
        activeQueries.erase(
            std::remove_if(
                activeQueries.begin(), activeQueries.end(), [queryId](const QueryId& id) { return id.getRawValue() == queryId; }),
            activeQueries.end());
    }
    catch (const std::exception& e)
    {
        std::cout << "Error stopping query: " << e.what() << "\n";
    }
}

void Repl::clearScreen()
{
    rx->clear_screen();
}

std::string Repl::getPrompt() const
{
    return "nebuli> ";
}

bool Repl::isCommand(const std::string& input) const
{
    std::istringstream iss(input);
    std::string cmd;
    iss >> cmd;

    return cmd == HELP_CMD || cmd == QUIT_CMD || cmd == EXIT_CMD || cmd == LIST_CMD || cmd == STOP_CMD || cmd == CLEAR_CMD;
}

void Repl::handleCommand(const std::string& input)
{
    std::istringstream iss(input);
    std::string cmd;
    iss >> cmd;

    if (cmd == HELP_CMD)
    {
        printHelp();
    }
    else if (cmd == QUIT_CMD || cmd == EXIT_CMD)
    {
        std::cout << "Goodbye!\n";
        exit(0);
    }
    else if (cmd == LIST_CMD)
    {
        printActiveQueries();
    }
    else if (cmd == STOP_CMD)
    {
        stopQuery(input);
    }
    else if (cmd == CLEAR_CMD)
    {
        clearScreen();
    }
}

std::string Repl::readMultiLineQuery()
{
    std::string query;
    std::string line;
    int parenCount = 0;
    bool inString = false;
    char stringChar = 0;

    do
    {
        if (!interactiveMode)
        {
            /// Use std::getline for non-interactive mode
            std::getline(std::cin, line);
            if (std::cin.eof())
            {
                break;
            }
        }
        else
        {
            /// Use Replxx for interactive mode
            line = rx->input(getPrompt());
        }

        if (line.empty())
        {
            continue;
        }

        if (interactiveMode)
        {
            rx->history_add(line);
        }

        for (char c : line)
        {
            if (inString)
            {
                if (c == stringChar)
                {
                    inString = false;
                    stringChar = 0;
                }
            }
            else
            {
                if (c == '\'' || c == '"')
                {
                    inString = true;
                    stringChar = c;
                }
                else if (c == '(')
                {
                    parenCount++;
                }
                else if (c == ')')
                {
                    parenCount--;
                }
            }
        }

        query += line + "\n";

        if (parenCount > 0 || inString)
        {
            continue;
        }

        /// Check if the line ends with a semicolon
        if (!line.empty() && line.back() == ';')
        {
            break;
        }

    } while (true);

    return query;
}

LogicalPlan Repl::parseAndOptimizeQuery(const std::string& query)
{
    auto sourceCatalog = std::make_shared<SourceCatalog>();
    auto yamlBinder = YAMLBinder{sourceCatalog};
    auto optimizer = LegacyOptimizer{sourceCatalog};
    auto logicalPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
    return optimizer.optimize(logicalPlan);
}

bool Repl::executeQuery(const std::string& query)
{
    try
    {
        auto optimizedPlan = parseAndOptimizeQuery(query);

        auto queryId = grpcClient->registerQuery(optimizedPlan);
        std::cout << "Query registered with ID: " << queryId << "\n";

        grpcClient->start(QueryId{queryId});
        std::cout << "Query started successfully.\n";

        activeQueries.emplace_back(queryId);

        return true;
    }
    catch (const std::exception& e)
    {
        std::cout << "Error executing query: " << e.what() << "\n";
        return false;
    }
}

void Repl::run()
{
    printWelcome();

    while (true)
    {
        try
        {
            std::string input;

            if (!interactiveMode)
            {
                /// Use std::getline for non-interactive mode to avoid terminal issues
                std::cout << getPrompt();
                std::getline(std::cin, input);
                if (std::cin.eof())
                {
                    break;
                }
            }
            else
            {
                /// Use Replxx for interactive mode
                input = rx->input(getPrompt());
            }

            if (input.empty())
            {
                continue;
            }

            /// Add to history (only in interactive mode)
            if (interactiveMode)
            {
                rx->history_add(input);
            }

            /// Check if it's a command
            if (isCommand(input))
            {
                handleCommand(input);
                continue;
            }

            /// Check if it's a single-line SQL query
            if (input.find("SELECT") == 0 || input.find("select") == 0)
            {
                if (executeQuery(input))
                {
                    std::cout << "Query executed successfully.\n";
                }
            }
            else
            {
                std::string fullQuery = input + "\n" + readMultiLineQuery();
                if (executeQuery(fullQuery))
                {
                    std::cout << "Query executed successfully.\n";
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "Error: " << e.what() << "\n";
        }
    }

    if (interactiveMode)
    {
        rx->history_save(".nebuli_history");
    }
}

}
