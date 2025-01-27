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
#include <exception>
#include <memory>
#include <string>
#include <Exceptions/ErrorListener.hpp>

namespace NES::Exceptions
{

/**
 * @brief calls to this function will pass the signal to all system-wide error listeners
 * @param signal which indicates the error
 * @param stacktrace the stacktrace of where the error was raised
 */
[[noreturn]] void invokeErrorHandlers(int signal, std::string&& stacktrace);

/**
 * @brief calls to this function will pass an exception to all system-wide error listeners
 * @param exception which indicates the error
 * @param stacktrace the stacktrace of where the error was raised
 */
[[noreturn]] void invokeErrorHandlers(std::shared_ptr<std::exception> exception, std::string&& stacktrace);

/**
 * @brief calls to this function will create a RuntimeException that is passed to all system-wide error listeners
 * @param buffer the message of the exception
 * @param stacktrace the stacktrace of where the error was raised
 */
[[noreturn]] void invokeErrorHandlers(const std::string& buffer, std::string&& stacktrace);

/**
 * @brief make an error listener system-wide
 * @param listener the error listener to make system-wide
 */
void installGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener);

/**
 * @brief remove an error listener system-wide
 * @param listener the error listener to remove system-wide
 */
void removeGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener);

}
