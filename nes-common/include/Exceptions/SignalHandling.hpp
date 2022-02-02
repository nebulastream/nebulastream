#ifndef NES_NES_COMMON_INCLUDE_EXCEPTIONS_SIGNALHANDLING_HPP_
#define NES_NES_COMMON_INCLUDE_EXCEPTIONS_SIGNALHANDLING_HPP_
#include <exception>
#include <memory>

namespace NES::Exceptions {
class ErrorListener;

/**
 * @brief calls to this function will pass the signal to all system-wide error listeners
 * @param signal which indicates the error
 * @param stacktrace the stacktrace of where the error was raised
 */
void invokeErrorHandlers(int signal, std::string&& stacktrace);

/**
 * @brief calls to this function will pass an exception to all system-wide error listeners
 * @param exception which indicates the error
 * @param stacktrace the stacktrace of where the error was raised
 */
void invokeErrorHandlers(std::shared_ptr<std::exception> exception, std::string&& stacktrace);

/**
 * @brief calls to this function will create a RuntimeException that is passed to all system-wide error listeners
 * @param buffer the message of the exception
 * @param stacktrace the stacktrace of where the error was raised
 */
void invokeErrorHandlers(const std::string& buffer, std::string&& stacktrace);

/**
 * @brief make an error listener system-wide
 * @param listener the error listener to make system-wide
 */
void installGlobalErrorListener(std::shared_ptr<ErrorListener> const& listener);

/**
 * @brief remove an error listener system-wide
 * @param listener the error listener to remove system-wide
 */
void removeGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener);

}// namespace NES

#endif//NES_NES_COMMON_INCLUDE_EXCEPTIONS_SIGNALHANDLING_HPP_
