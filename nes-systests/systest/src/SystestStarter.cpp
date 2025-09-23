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

#include <chrono>
#include <iostream>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SystestExecutor.hpp>

int main(int argc, const char** argv)
{
    auto startTime = std::chrono::high_resolution_clock::now();

    switch (const auto [returnType, outputMessage, exceptionCode] = NES::executeSystests(NES::readConfiguration(argc, argv)); returnType)
    {
        case SystestExecutorResult::ReturnType::SUCCESS: {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            std::cout << outputMessage << '\n';
            std::cout << "Total execution time: " << duration.count() << " ms ("
                      << std::chrono::duration_cast<std::chrono::seconds>(duration).count() << " seconds)" << '\n';
            return 0;
        }
        case SystestExecutorResult::ReturnType::FAILED: {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            PRECONDITION(exceptionCode, "Returning with as 'FAILED_WITH_EXCEPTION_CODE', but did not provide error code");
            NES_ERROR("{}", outputMessage);
            std::cout << outputMessage << '\n';
            std::cout << "Total execution time: " << duration.count() << " ms ("
                      << std::chrono::duration_cast<std::chrono::seconds>(duration).count() << " seconds)" << '\n';
            return static_cast<int>(exceptionCode.value());
        }
    }
}
