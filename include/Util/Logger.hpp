/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_UTIL_LOGGER_HPP_
#define INCLUDE_UTIL_LOGGER_HPP_
// TRACE < DEBUG < INFO < WARN < ERROR < FATAL

#include <Util/StacktraceLoader.hpp>
#include <iostream>
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>

using namespace log4cxx;
using namespace log4cxx::helpers;

namespace NES {

enum DebugLevel { LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE };

static std::string getDebugLevelAsString(DebugLevel level) {
    if (level == LOG_NONE) {
        return "LOG_NONE";
    } else if (level == LOG_WARNING) {
        return "LOG_WARNING";
    } else if (level == LOG_DEBUG) {
        return "LOG_DEBUG";
    } else if (level == LOG_INFO) {
        return "LOG_INFO";
    } else if (level == LOG_TRACE) {
        return "LOG_TRACE";
    } else {
        return "UNKNOWN";
    }
}

static DebugLevel getStringAsDebugLevel(std::string level) {
    if (level == "LOG_NONE") {
        return LOG_NONE;
    } else if (level == "LOG_WARNING") {
        return LOG_WARNING;
    } else if (level == "LOG_DEBUG") {
        return LOG_DEBUG;
    } else if (level == "LOG_INFO") {
        return LOG_INFO;
    } else if (level == "LOG_TRACE") {
        return LOG_TRACE;
    } else {
        throw std::runtime_error("Logger: Debug level unknown: " + level);
    }
}

static log4cxx::LoggerPtr NESLogger(log4cxx::Logger::getLogger("NES"));

// LoggerPtr logger(Logger::getLogger("NES"));

// TRACE < DEBUG < INFO < WARN < ERROR < FATAL
#define LEVEL_TRACE 6
#define LEVEL_DEBUG 5
#define LEVEL_INFO 4
#define LEVEL_WARN 3
#define LEVEL_ERROR 2
#define LEVEL_FATAL 1

#ifdef NES_LOGGING_LEVEL
#if NES_LOGGING_LEVEL >= LEVEL_TRACE
#define NES_TRACE(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_TRACE(NESLogger, TEXT);                                                                                          \
    } while (0)
#else
#define NES_TRACE(TEXT) ((void) 0)
#endif

#if NES_LOGGING_LEVEL >= LEVEL_DEBUG
#define NES_DEBUG(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_DEBUG(NESLogger, TEXT);                                                                                          \
    } while (0)
#else
#define NES_DEBUG(TEXT) ((void) 0)
#endif

#if NES_LOGGING_LEVEL >= LEVEL_INFO
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        LOG4CXX_INFO(NESLogger, TEXT);                                                                                           \
    } while (0)
#else
#define NES_INFO(TEXT) ((void) 0)
#endif

#if NES_LOGGING_LEVEL >= LEVEL_WARN
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NESLogger, TEXT);                                                                                           \
    } while (0)
#else
#define NES_WARNING(TEXT) ((void) 0)
#endif

#if NES_LOGGING_LEVEL >= LEVEL_ERROR
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NESLogger, TEXT);                                                                                          \
    } while (0)
#else
#define NES_ERROR(TEXT) ((void) 0)
#endif

#if NES_LOGGING_LEVEL >= LEVEL_FATAL
#define NES_FATAL_ERROR(TEXT)                                                                                                    \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NESLogger, TEXT);                                                                                          \
    } while (0)
#else
#define NES_FATAL_ERROR(TEXT) ((void) 0)
#endif
#else
#define NES_TRACE(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_TRACE(NESLogger, TEXT);                                                                                          \
    } while (0)
#define NES_DEBUG(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_DEBUG(NESLogger, TEXT);                                                                                          \
    } while (0)
#define NES_TRACE(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_TRACE(NESLogger, TEXT);                                                                                          \
    } while (0)
#define NES_INFO(TEXT)                                                                                                           \
    do {                                                                                                                         \
        LOG4CXX_INFO(NESLogger, TEXT);                                                                                           \
    } while (0)
#define NES_WARNING(TEXT)                                                                                                        \
    do {                                                                                                                         \
        LOG4CXX_WARN(NESLogger, TEXT);                                                                                           \
    } while (0)
#define NES_ERROR(TEXT)                                                                                                          \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NESLogger, TEXT);                                                                                          \
    } while (0)
#define NES_FATAL_ERROR(TEXT)                                                                                                    \
    do {                                                                                                                         \
        LOG4CXX_ERROR(NESLogger, TEXT);                                                                                          \
    } while (0)
#endif

#ifdef NES_DEBUG
#define NES_VERIFY(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            NES::collectAndPrintStacktrace();                                                                                    \
            NES_FATAL_ERROR(TEXT);                                                                                               \
            throw std::runtime_error("NES Runtime Error on condition " #CONDITION);                                              \
        }                                                                                                                        \
    } while (0)
#else
#define NES_VERIFY(CONDITION, TEXT) ((void) 0)
#endif

#define NES_ASSERT(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            NES::collectAndPrintStacktrace();                                                                                    \
            NES_FATAL_ERROR(TEXT);                                                                                               \
            throw std::runtime_error("NES Runtime Error on condition " #CONDITION);                                              \
        }                                                                                                                        \
    } while (0)

#define NES_THROW_RUNTIME_ERROR(TEXT)                                                                                            \
    do {                                                                                                                         \
        NES::collectAndPrintStacktrace();                                                                                        \
        NES_FATAL_ERROR(TEXT);                                                                                                   \
        throw std::runtime_error(TEXT);                                                                                          \
    } while (0)

static void setupLogging(std::string logFileName, DebugLevel level) {
    std::cout << "Logger: SETUP_LOGGING" << std::endl;
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c: %l %X{threadName} [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, logFileName);
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
#ifdef NES_LOGGING_LEVEL
#if NES_LOGGING_LEVEL == LEVEL_FATAL
    NESLogger->setLevel(log4cxx::Level::getFatal());
#endif
#if NES_LOGGING_LEVEL == LEVEL_ERROR
    NESLogger->setLevel(log4cxx::Level::getError());
#endif
#if NES_LOGGING_LEVEL == LEVEL_WARN
    NESLogger->setLevel(log4cxx::Level::getWarn());
#endif
#if NES_LOGGING_LEVEL == LEVEL_INFO
    NESLogger->setLevel(log4cxx::Level::getInfo());
#endif
#if NES_LOGGING_LEVEL == LEVEL_DEBUG
    NESLogger->setLevel(log4cxx::Level::getDebug());
#endif
#if NES_LOGGING_LEVEL == LEVEL_TRACE
    NESLogger->setLevel(log4cxx::Level::getTrace());
#endif
#else
    if (level == LOG_NONE) {
        NESLogger->setLevel(log4cxx::Level::getOff());
    } else if (level == LOG_WARNING) {
        NESLogger->setLevel(log4cxx::Level::getWarn());
    } else if (level == LOG_DEBUG) {
        NESLogger->setLevel(log4cxx::Level::getDebug());
    } else if (level == LOG_INFO) {
        NESLogger->setLevel(log4cxx::Level::getInfo());
    } else if (level == LOG_TRACE) {
        NESLogger->setLevel(log4cxx::Level::getTrace());
    } else {
        NES_ERROR("setupLogging: log level not supported " << getDebugLevelAsString(level));
        throw Exception("Error while setup logging");
    }
#endif

    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
}

static void setLogLevel(DebugLevel level){
    // set log level
#ifdef NES_LOGGING_LEVEL
    #if NES_LOGGING_LEVEL == LEVEL_FATAL
    NESLogger->setLevel(log4cxx::Level::getFatal());
#endif
#if NES_LOGGING_LEVEL == LEVEL_ERROR
    NESLogger->setLevel(log4cxx::Level::getError());
#endif
#if NES_LOGGING_LEVEL == LEVEL_WARN
    NESLogger->setLevel(log4cxx::Level::getWarn());
#endif
#if NES_LOGGING_LEVEL == LEVEL_INFO
    NESLogger->setLevel(log4cxx::Level::getInfo());
#endif
#if NES_LOGGING_LEVEL == LEVEL_DEBUG
    NESLogger->setLevel(log4cxx::Level::getDebug());
#endif
#if NES_LOGGING_LEVEL == LEVEL_TRACE
    NESLogger->setLevel(log4cxx::Level::getTrace());
#endif
#else
    if (level == LOG_NONE) {
        NESLogger->setLevel(log4cxx::Level::getOff());
    } else if (level == LOG_WARNING) {
        NESLogger->setLevel(log4cxx::Level::getWarn());
    } else if (level == LOG_DEBUG) {
        NESLogger->setLevel(log4cxx::Level::getDebug());
    } else if (level == LOG_INFO) {
        NESLogger->setLevel(log4cxx::Level::getInfo());
    } else if (level == LOG_TRACE) {
        NESLogger->setLevel(log4cxx::Level::getTrace());
    } else {
        NES_ERROR("setLogLevel: log level not supported " << getDebugLevelAsString(level));
        throw Exception("Error while trying to change log level");
    }
#endif
}

#define NES_NOT_IMPLEMENTED()                                                                                                    \
    do {                                                                                                                         \
        NES_ERROR("Function Not Implemented!");                                                                                  \
        throw Exception("not implemented");                                                                                      \
    } while (0)

}// namespace NES

#endif /* INCLUDE_UTIL_LOGGER_HPP_ */
