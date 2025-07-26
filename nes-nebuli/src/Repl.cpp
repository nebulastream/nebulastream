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

#include <algorithm>
#include <array>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <unistd.h>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <YAML/YAMLBinder.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <LegacyOptimizer.hpp>
#include <replxx.hxx>

namespace NES
{

Repl::Repl(std::shared_ptr<GRPCClient> client)
    : grpcClient(std::move(client)), interactiveMode((isatty(STDIN_FILENO) != 0) and (isatty(STDOUT_FILENO) != 0))
{
    if (interactiveMode)
    {
        setupReplxx();
    }
    else
    {
        std::cout << "Non-interactive mode detected (not a TTY). Using basic input mode.\n";
    }
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

            const std::vector<std::string> commands = {"help", "quit", "exit", "clear"};
            for (const auto& cmd : commands)
            {
                if (input.starts_with(cmd))
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
    const bool useColour = isatty(STDOUT_FILENO) != 0;
    auto color = [&](const char* esc) { return useColour ? esc : ""; };
    const char* bold = color("\033[1m");
    const char* accent = color("\033[34m");
    const char* reset = color("\033[0m");

    constexpr std::string_view title = "NebulaStream Interactive Query Shell";
    constexpr std::size_t width = 60;
    const std::size_t pad = (width - title.size()) / 2;

    std::cout << '\n' << accent << std::string(width, '=') << '\n';
    std::cout << std::string(pad, ' ') << bold << title << reset << '\n';
    std::cout << accent << std::string(width, '=') << reset << '\n';

    struct Cmd
    {
        const char* name;
        const char* desc;
    };
    constexpr std::array<Cmd, 4> cmds{
        {{.name = "help", .desc = "Show this help message"},
         {.name = "clear", .desc = "Clear the screen"},
         {.name = "quit", .desc = "Exit the shell"},
         {.name = "exit", .desc = "Alias for quit"}}};

    std::cout << bold << "Commands" << reset << ":\n";
    for (auto [name, desc] : cmds)
    {
        std::cout << "  • " << bold << name << reset << std::string(8 - std::strlen(name), ' ') << "─ " << desc << '\n';
    }
    std::cout << '\n'
              << "Enter SQL to execute it; multi‑line statements are supported and\n"
              << "run automatically once the final line ends with a semicolon.\n\n";
}

void Repl::printHelp()
{
    const bool useColour = isatty(STDOUT_FILENO) != 0;
    auto color = [&](const char* esc) { return useColour ? esc : ""; };

    const char* bold = color("\033[1m");
    const char* reset = color("\033[0m");
    const char* accent = color("\033[34m");

    struct Cmd
    {
        const char* name;
        const char* desc;
    };
    constexpr std::array<Cmd, 4> cmds{
        {{.name = "help", .desc = "Show this help message"},
         {.name = "clear", .desc = "Clear the screen"},
         {.name = "quit", .desc = "Exit the shell"},
         {.name = "exit", .desc = "Alias for quit"}}};

    std::size_t padWidth = 0;
    for (const auto& cmd : cmds)
    {
        padWidth = std::max(padWidth, std::strlen(cmd.name));
    }
    padWidth += 2;

    std::cout << '\n' << bold << "Commands" << reset << ":\n";
    for (const auto& cmd : cmds)
    {
        std::cout << "  " << bold << cmd.name << reset << std::string(padWidth - std::strlen(cmd.name), ' ') << "─ " << cmd.desc << '\n';
    }

    std::cout << '\n'
              << "Enter SQL to execute it; multi‑line statements are supported and\n"
              << "run automatically once the final line ends with a semicolon.\n\n"
              << "Docs: " << accent << "https://docs.nebula.stream/" << reset << "\n\n";
}

void Repl::clearScreen()
{
    constexpr const char* ansiClear = "\033[2J\033[H";
    if (interactiveMode)
    {
        rx->clear_screen();
    }
    else
    {
#ifdef _WIN32
        std::cout << ansiClear << std::flush;
#else
        std::cout << ansiClear << std::flush;
#endif
    }
}

std::string Repl::getPrompt()
{
    return "NES 🌌 > ";
}

bool Repl::isCommand(const std::string& input)
{
    std::istringstream iss(input);
    std::string cmd;
    iss >> cmd;

    return cmd == HELP_CMD || cmd == QUIT_CMD || cmd == EXIT_CMD || cmd == CLEAR_CMD;
}

bool Repl::handleCommand(const std::string& input)
{
    std::istringstream iss(input);
    std::string cmd;
    iss >> cmd;

    if (cmd == HELP_CMD)
    {
        printHelp();
        return false;
    }

    if (cmd == QUIT_CMD || cmd == EXIT_CMD)
    {
        std::cout << "Goodbye!\n";
        return true;
    }

    if (cmd == CLEAR_CMD)
    {
        clearScreen();
        return false;
    }
    return false;
}

std::string Repl::readMultiLineQuery() const
{
    std::string query;
    std::string line;
    size_t parenCount = 0;
    bool inString = false;
    char stringChar = 0;

    while (true)
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

        for (const char charInLine : line)
        {
            if (inString)
            {
                if (charInLine == stringChar)
                {
                    inString = false;
                    stringChar = 0;
                }
            }
            else
            {
                if (charInLine == '\'' || charInLine == '"')
                {
                    inString = true;
                    stringChar = charInLine;
                }
                else if (charInLine == '(')
                {
                    parenCount++;
                }
                else if (charInLine == ')')
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
    }
    return query;
}

bool Repl::executeQuery(const std::string& query)
{
    try
    {
        auto yamlBinder = CLI::YAMLBinder{sourceCatalog, sinkCatalog};
        auto optimizer = CLI::LegacyOptimizer{sourceCatalog, sinkCatalog};
        auto logicalPlan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query);
        auto optimizedPlan = optimizer.optimize(logicalPlan);

        auto queryId = grpcClient->registerQuery(optimizedPlan);
        std::cout << "Query registered with ID: " << queryId << "\n";

        grpcClient->start(QueryId{queryId});
        std::cout << "Query started successfully.\n";
        return true;
    }
    catch (const Exception& e)
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
                if (handleCommand(input))
                {
                    break;
                }
                continue;
            }

            /// Check if it's a single-line SQL query
            auto trim = [](const std::string& str) -> std::string
            {
                const size_t start = str.find_first_not_of(" \t\n\r");
                if (start == std::string::npos)
                {
                    return "";
                }
                const size_t end = str.find_last_not_of(" \t\n\r");
                return str.substr(start, end - start + 1);
            };
            auto isCompleteStatement = [&](const std::string& stmt) -> bool
            {
                std::string trimmed = trim(stmt);
                return !trimmed.empty() && trimmed.back() == ';';
            };
            if (isCompleteStatement(input))
            {
                if (executeQuery(input))
                {
                    std::cout << "Query executed successfully.\n";
                }
            }
            else
            {
                const std::string fullQuery = input + "\n" + readMultiLineQuery();
                if (executeQuery(fullQuery))
                {
                    std::cout << "Query executed successfully.\n";
                }
            }
        }
        catch (const Exception& e)
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
