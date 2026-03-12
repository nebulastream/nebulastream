#pragma once

#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include <Util/Logger/Logger.hpp>

namespace NES::SystestTupleCrashControl
{
inline constexpr auto EnabledEnvironmentVariable = "NES_SYSTEST_CRASH_AFTER_EVERY_TUPLE";
inline constexpr auto CrashFrequencyEnvironmentVariable = "NES_SYSTEST_CRASH_EVERY_N_TUPLES";
inline constexpr auto CrashExitCode = 86;
inline constexpr int ArmSignal = SIGUSR1;
inline constexpr int DisarmSignal = SIGUSR2;

inline volatile std::sig_atomic_t enabled = 0;
inline volatile std::sig_atomic_t armed = 0;
inline volatile std::sig_atomic_t initialized = 0;
inline std::uint64_t crashEveryNTuples = 1;

inline void armHandler(int)
{
    armed = 1;
}

inline void disarmHandler(int)
{
    armed = 0;
}

inline std::uint64_t parseCrashFrequency()
{
    const auto* envValue = std::getenv(CrashFrequencyEnvironmentVariable);
    if (envValue == nullptr)
    {
        return 1;
    }

    std::uint64_t parsedValue = 0;
    const auto* end = envValue + std::strlen(envValue);
    const auto [ptr, error] = std::from_chars(envValue, end, parsedValue);
    if (error == std::errc{} && ptr == end && parsedValue > 0)
    {
        return parsedValue;
    }

    return 1;
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
    crashEveryNTuples = parseCrashFrequency();
    std::signal(ArmSignal, armHandler);
    std::signal(DisarmSignal, disarmHandler);
}

inline bool isEnabled()
{
    return enabled != 0;
}

inline void beforeTuple()
{
}

[[nodiscard]] inline std::uint64_t getCrashFrequency()
{
    return crashEveryNTuples;
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
