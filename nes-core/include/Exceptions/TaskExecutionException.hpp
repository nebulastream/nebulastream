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

#ifndef NES_CORE_INCLUDE_EXCEPTIONS_TASKEXECUTIONEXCEPTION_HPP_
#define NES_CORE_INCLUDE_EXCEPTIONS_TASKEXECUTIONEXCEPTION_HPP_

#include <Exceptions/RuntimeException.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <string>

namespace NES {

/**
 * @brief This exception is thrown when an error occurs during window processing.
 */
class TaskExecutionException : public Exceptions::RuntimeException {
  public:
    /**
     * @brief Construct a PipelineExecutionException exception from a message.
     * @param message The exception message.
     */
    explicit TaskExecutionException(const Runtime::Execution::SuccessorExecutablePipeline pipeline, std::string&& message);

    Runtime::Execution::SuccessorExecutablePipeline getExecutable() const;

  private:
    const std::string message;
    const Runtime::Execution::SuccessorExecutablePipeline pipeline;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_EXCEPTIONS_TASKEXECUTIONEXCEPTION_HPP_
