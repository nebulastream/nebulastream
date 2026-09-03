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

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

class FaultTrigger
{
public:
    virtual ~FaultTrigger() = default;

    virtual bool eval() = 0;

    std::atomic<bool> triggered;
};

class AlwaysFaultTrigger : public FaultTrigger
{
public:
    bool eval() override;
};

class TimeBasedFaultTrigger : public FaultTrigger
{
public:
    TimeBasedFaultTrigger(std::chrono::system_clock::time_point startupTime, std::chrono::system_clock::duration delay);

    bool eval() override;

private:
    std::atomic<bool> triggered{false};
    std::chrono::system_clock::time_point targetTime;
};

class TimeFrameFaultTrigger : public FaultTrigger
{
public:
    TimeFrameFaultTrigger(
        std::chrono::system_clock::time_point startupTime,
        std::chrono::system_clock::duration start,
        std::chrono::system_clock::duration end);

    bool eval() override;

private:
    std::chrono::system_clock::time_point start;
    std::chrono::system_clock::time_point end;
};

class CountBasedFaultTrigger : public FaultTrigger
{
public:
    explicit CountBasedFaultTrigger(uint64_t targetCount);

    bool eval() override;

private:
    std::atomic<uint64_t> currentCount{0};
    uint64_t targetCount;
};

enum class FaultAction : uint8_t
{
    CRASH = 0,
    DISCONNECT = 1,
    UDF = 2,
    NONE = 3,
};

struct FaultRule
{
    std::unique_ptr<FaultTrigger> trigger;
    FaultAction action;
};

class FaultSimulator
{
public:
    bool check();

    void simulateCrash();
    void simulateDisconnect();
    void release();

    void setCrashCallback(std::function<void()> callback);

private:
    std::atomic<bool> triggered{false};
    std::function<void()> crashCallback;
};

class FaultInjectionContext
{
public:
    void configure(std::string configYaml);

    bool evalFailpoint(std::string_view failpointName);

    std::optional<FaultAction> evalAction(std::string_view failpointName);

    void applyAction(FaultAction action);

    std::vector<std::string> pendingFailpoints();

    Host host;

    FaultInjectionContext(Host host) : host(host) { }

    FaultSimulator simulator;
    std::unordered_map<std::string, std::vector<FaultRule>> rules;
    std::unordered_map<std::string, std::atomic_uint64_t> hitCountStats;
    std::chrono::system_clock::time_point firstStartup;
};

FaultInjectionContext* getActiveFaultContext();

void initActiveFaultContext(Host host);

class FaultContextRegistry
{
public:
    static FaultContextRegistry& instance();

    FaultInjectionContext* getFaultContext(Host host);

    void configure();

    FaultContextRegistry(const FaultContextRegistry&) = delete;
    FaultContextRegistry& operator=(const FaultContextRegistry&) = delete;

private:
    FaultContextRegistry() = default;

    std::mutex mutex;
    std::unordered_map<Host, std::unique_ptr<FaultInjectionContext>> faultContexts;
};

inline bool checkIO()
{
    auto* context = getActiveFaultContext();
    return context->simulator.check();
}

inline bool failpoint(std::string_view name)
{
    auto* context = getActiveFaultContext();
    return context->evalFailpoint(name);
}

inline std::optional<FaultAction> deferredFailpoint(std::string_view name)
{
    return getActiveFaultContext()->evalAction(name);
}

inline void applyFaultAction(FaultAction action)
{
    getActiveFaultContext()->applyAction(action);
}

#ifdef FAULT_TESTING

    #define CHECK_IO() checkIO()
    #define FAILPOINT(name) failpoint(name)
    #define DEFERRED_FAILPOINT(name) NES::deferredFailpoint(name)
    #define APPLY_FAULT_ACTION(action) NES::applyFaultAction(action)

#else

    #define CHECK_IO() false
    #define FAILPOINT(name) false
    #define DEFERRED_FAILPOINT(name) (std::optional<NES::FaultAction>{})
    #define APPLY_FAULT_ACTION(action) ((void)0)

#endif

}