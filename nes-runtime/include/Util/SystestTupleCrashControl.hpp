#pragma once

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string_view>
#include <thread>

#include <Util/Logger/Logger.hpp>

namespace NES::SystestTupleCrashControl
{
inline constexpr auto EnabledEnvironmentVariable = "NES_SYSTEST_CRASH_AFTER_EVERY_TUPLE";
inline constexpr auto PauseOnStartupEnvironmentVariable = "NES_SYSTEST_PAUSE_UNTIL_ARM";
inline constexpr auto CrashExitCode = 86;
inline constexpr int ArmSignal = SIGUSR1;
inline constexpr int DisarmSignal = SIGUSR2;

inline volatile std::sig_atomic_t enabled = 0;
inline volatile std::sig_atomic_t armed = 0;
inline volatile std::sig_atomic_t pauseUntilArmed = 0;
inline volatile std::sig_atomic_t initialized = 0;

inline void armHandler(int)
{
    armed = 1;
    pauseUntilArmed = 0;
}

inline void disarmHandler(int)
{
    armed = 0;
    pauseUntilArmed = 1;
}

inline void initializeFromEnvironment()
{
    if (initialized)
    {
        return;
    }
    initialized = 1;

    const auto* envValue = std::getenv(EnabledEnvironmentVariable);
    if (envValue == nullptr || std::string_view(envValue) != "1")
    {
        return;
    }

    enabled = 1;
    armed = 0;
    const auto* pauseValue = std::getenv(PauseOnStartupEnvironmentVariable);
    pauseUntilArmed = (pauseValue != nullptr && std::string_view(pauseValue) == "1") ? 1 : 0;
    std::signal(ArmSignal, armHandler);
    std::signal(DisarmSignal, disarmHandler);
}

inline bool isEnabled()
{
    return enabled != 0;
}

inline void beforeTuple()
{
    if (!isEnabled())
    {
        return;
    }

    while (pauseUntilArmed != 0 && armed == 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

inline void afterTuple()
{
    if (!isEnabled() || armed == 0)
    {
        return;
    }

    if (auto logger = Logger::getInstance())
    {
        logger->forceFlush();
    }
    std::_Exit(CrashExitCode);
}
}
