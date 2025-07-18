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
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <Plans/LogicalPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceCatalog.hpp>
#include <YAML/YAMLBinder.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <LegacyOptimizer.hpp>

namespace NES
{

Repl::Repl(std::shared_ptr<GRPCClient> client) : grpcClient(std::move(client))
{
    interactiveMode = isatty(STDIN_FILENO) and isatty(STDOUT_FILENO);
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

            std::vector<std::string> commands = {"help", "quit", "exit", "clear"};
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
    const bool useColour = isatty(STDOUT_FILENO);
    auto c = [&](const char* esc) { return useColour ? esc : ""; };
    const char* BOLD = c("\033[1m");
    const char* ACCENT = c("\033[34m");
    const char* RESET = c("\033[0m");

    constexpr std::string_view title = "NebulaStream Interactive Query Shell";
    constexpr std::size_t width = 60;
    const std::size_t pad = (width - title.size()) / 2;

    std::cout << '\n' << ACCENT << std::string(width, '=') << '\n';
    std::cout << std::string(pad, ' ') << BOLD << title << RESET << '\n';
    std::cout << ACCENT << std::string(width, '=') << RESET << '\n';

    struct Cmd
    {
        const char* name;
        const char* desc;
    };
    const Cmd cmds[]{{"help", "Show this help"}, {"clear", "Clear the screen"}, {"quit", "Exit the shell"}, {"exit", "Alias for quit"}};

    std::cout << BOLD << "Commands" << RESET << ":\n";
    for (auto [name, desc] : cmds)
    {
        std::cout << "  • " << BOLD << name << RESET << std::string(8 - std::strlen(name), ' ') << "─ " << desc << '\n';
    }
    std::cout << '\n'
              << "Enter SQL to execute it; multi‑line statements are supported and\n"
              << "run automatically once the final line ends with a semicolon.\n\n";
}

void Repl::printHelp()
{
    const bool useColour = isatty(STDOUT_FILENO);
    auto cc = [&](const char* esc) { return useColour ? esc : ""; };

    const char* BOLD = cc("\033[1m");
    const char* RESET = cc("\033[0m");
    const char* ACCENT = cc("\033[34m");

    struct Cmd
    {
        const char* name;
        const char* desc;
    };
    constexpr Cmd cmds[]{
        {"help", "Show this help message"}, {"clear", "Clear the screen"}, {"quit", "Exit the shell"}, {"exit", "Alias for quit"}};

    std::size_t padWidth = 0;
    for (auto& c : cmds)
        padWidth = std::max(padWidth, std::strlen(c.name));
    padWidth += 2;

    std::cout << '\n' << BOLD << "Commands" << RESET << ":\n";
    for (auto& c : cmds)
    {
        std::cout << "  " << BOLD << c.name << RESET << std::string(padWidth - std::strlen(c.name), ' ') << "─ " << c.desc << '\n';
    }

    std::cout << '\n'
              << "Enter SQL to execute it; multi‑line statements are supported and\n"
              << "run automatically once the final line ends with a semicolon.\n\n"
              << "Docs: " << ACCENT << "https://docs.nebula.stream/" << RESET << "\n\n";
}

void Repl::clearScreen()
{
    constexpr const char* ANSI_CLEAR = "\033[2J\033[H";
    if (interactiveMode)
    {
        rx->clear_screen();
    }
    else
    {
#ifdef _WIN32
        std::system("cls");
#else
        std::cout << ANSI_CLEAR << std::flush;
        if (std::system("clear") != 0)
        {
            std::cout << std::string(50, '\n');
        }
#endif
    }
}

std::string Repl::getPrompt() const
{
    return "NES 🌌 > ";
}

bool Repl::isCommand(const std::string& input) const
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

bool Repl::executeQuery(const std::string& query)
{
    try
    {
        auto yamlBinder = CLI::YAMLBinder{sourceCatalog};
        auto optimizer = CLI::LegacyOptimizer{sourceCatalog};
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
            auto trim = [](const std::string& s) -> std::string
            {
                size_t start = s.find_first_not_of(" \t\n\r");
                if (start == std::string::npos)
                    return "";
                size_t end = s.find_last_not_of(" \t\n\r");
                return s.substr(start, end - start + 1);
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
                std::string fullQuery = input + "\n" + readMultiLineQuery();
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
