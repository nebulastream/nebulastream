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

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <Config/ConfigParser.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Signal.hpp>
#include <FaultConfigParser.hpp>
#include <SystestExecutor.hpp>

using namespace NES;

int main(int argc, const char** argv)
{
    #ifndef FAULT_TESTING
    std::cout << "fault Testing is not enabled"
    return 1;
    #endif

    if (argc != 2)
    {
        std::cout << "usage: <config_file_path>" << std::endl;
        return 1;
    }

    setupSignalHandlers();

    CPPTRACE_TRY
    {
        loadBuiltinPlugins();

        std::string configFile = argv[1];
        auto testCases = parseFaultConfigFile(configFile);
        if (testCases.empty())
        {
            std::cout << "no fault test cases in config file" << std::endl;
            return 1;
        }

        int failed = 0;

        for (auto& testCase : testCases)
        {
            std::vector<const char*> systestArgs;
            systestArgs.push_back("systest");
            if (not testCase.topologyConfig.empty())
            {
                systestArgs.push_back("-c");
                systestArgs.push_back(testCase.topologyConfig.c_str());
            }
            auto systestConfig = parseConfig(static_cast<int>(systestArgs.size()), systestArgs.data());

            for (auto& systestFile : testCase.systestFiles)
            {
                systestConfig.directlySpecifiedTestFiles.add(systestFile);
            }
            systestConfig.faultSimulationConfig = testCase.globalConfig;
            systestConfig.numberConcurrentQueries = 1;

            SystestExecutor executor(std::move(systestConfig));
            auto result = executor.executeSystests();

            if (result.returnType != SystestExecutorResult::ReturnType::SUCCESS)
            {
                ++failed;
            }
        }

        std::cout << "successful: " << testCases.size() - failed << " out of " << testCases.size() << std::endl;
        return failed == 0;
    }
    CPPTRACE_CATCH(const NES::Exception&)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
