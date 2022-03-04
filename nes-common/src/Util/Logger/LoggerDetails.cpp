
#include <Util/Logger/LoggerDetails.hpp>
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/helpers/pool.h>
#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>

namespace NES {

class CustomPatternLayout : public log4cxx::PatternLayout {
  public:
    CustomPatternLayout() : log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c: %l [%M] %X{threadName} [%-5t] [%p] : %m%n") {}
    void
    format(log4cxx::LogString& output, const log4cxx::spi::LoggingEventPtr& event, log4cxx::helpers::Pool& pool) const override {
        log4cxx::LogString tmp;
        log4cxx::PatternLayout::format(tmp, event, pool);
        log4cxx::LevelPtr lvl = event->getLevel();
        switch (lvl->toInt()) {
            case log4cxx::Level::FATAL_INT:
                output.append("\u001b[0;41m");//red BG
                break;
            case log4cxx::Level::ERROR_INT:
                output.append("\u001b[0;31m");// red FG
                break;
            case log4cxx::Level::WARN_INT:
                output.append("\u001b[0;33m");//Yellow FG
                break;
            case log4cxx::Level::INFO_INT:
                output.append("\u001b[1m");// Bright
                break;
            case log4cxx::Level::DEBUG_INT:
                output.append("\u001b[2;32m");// Green FG
                break;
            case log4cxx::Level::TRACE_INT:
                output.append("\u001b[0;30m");// Black FG
                break;
            default: break;
        }
        output.append(tmp);
        output.append("\u001b[m");
    }
};

LoggerDetails::LoggerDetails() : loggerPtr(log4cxx::Logger::getLogger("NES")) {}

void LoggerDetails::setLogLevel(const LogLevel logLevel) {
    switch (logLevel) {
        case LogLevel::LOG_NONE: loggerPtr->setLevel(log4cxx::Level::getOff()); break;
        case LogLevel::LOG_FATAL_ERROR: loggerPtr->setLevel(log4cxx::Level::getFatal()); break;
        case LogLevel::LOG_ERROR: loggerPtr->setLevel(log4cxx::Level::getError()); break;
        case LogLevel::LOG_WARNING: loggerPtr->setLevel(log4cxx::Level::getWarn()); break;
        case LogLevel::LOG_DEBUG: loggerPtr->setLevel(log4cxx::Level::getDebug()); break;
        case LogLevel::LOG_INFO: loggerPtr->setLevel(log4cxx::Level::getInfo()); break;
        case LogLevel::LOG_TRACE: loggerPtr->setLevel(log4cxx::Level::getTrace()); break;
    }
}

void LoggerDetails::setupLogging(const std::string& logFileName) {
    std::cout << "Logger: Setting up logger" << std::endl;
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new CustomPatternLayout());

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, logFileName);
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));
    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));
    loggerPtr->addAppender(file);
    loggerPtr->addAppender(console);
}
void LoggerDetails::setThreadName(const std::string threadName) { log4cxx::MDC::put("threadName", threadName); }
}// namespace NES