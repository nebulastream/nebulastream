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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using AsyncExecutorRegistryReturnType = std::unique_ptr<AsyncOperatorExecutor>;

struct AsyncExecutorRegistryArguments
{
    AsyncOperatorContext context;
};

using AsyncExecutorFactoryFn = std::function<AsyncExecutorRegistryReturnType(AsyncExecutorRegistryArguments)>;

/// Creates the registry entry for an executor implementation. Executors are constructed
/// from their operator context; the entry expression in cmake/RuntimeRegistrationUtil.cmake
/// instantiates this per plugin type.
template <typename ExecutorImpl>
AsyncExecutorFactoryFn makeAsyncExecutor()
{
    return [](AsyncExecutorRegistryArguments arguments) -> AsyncExecutorRegistryReturnType
    { return std::make_unique<ExecutorImpl>(std::move(arguments.context)); };
}

class AsyncExecutorRegistry : public RuntimeRegistry<AsyncExecutorRegistry, std::string, AsyncExecutorFactoryFn, /*CaseSensitive*/ false>
{
public:
    static AsyncExecutorRegistry& instance();
};

}
