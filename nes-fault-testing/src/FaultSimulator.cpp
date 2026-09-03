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

#include <FaultSimulator.hpp>

#include <iostream>
#include <mutex>
#include <FaultConfigParser.hpp>

namespace NES
{

bool AlwaysFaultTrigger::eval()
{
    return true;
}

TimeBasedFaultTrigger::TimeBasedFaultTrigger(std::chrono::system_clock::time_point startupTime, std::chrono::system_clock::duration delay)
    : targetTime(startupTime + delay)
{
}

bool TimeBasedFaultTrigger::eval()
{
    if (std::chrono::system_clock::now() < targetTime)
    {
        return false;
    }

    bool expected = false;
    return triggered.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed);
}

TimeFrameFaultTrigger::TimeFrameFaultTrigger(
    std::chrono::system_clock::time_point startupTime, std::chrono::system_clock::duration start, std::chrono::system_clock::duration end)
    : start(startupTime + start), end(startupTime + end)
{
}

bool TimeFrameFaultTrigger::eval()
{
    auto now = std::chrono::system_clock::now();
    return now >= start && now < end;
}

CountBasedFaultTrigger::CountBasedFaultTrigger(uint64_t targetCount) : targetCount(targetCount)
{
}

bool CountBasedFaultTrigger::eval()
{
    auto count = currentCount.fetch_add(1, std::memory_order_relaxed) + 1;
    return count == targetCount;
}

bool FaultSimulator::check()
{
    return triggered.load(std::memory_order_acquire);
}

void FaultSimulator::simulateCrash()
{
    triggered.store(true, std::memory_order_release);

    std::thread(
        [this]
        {
            crashCallback();
            std::this_thread::sleep_for(std::chrono::seconds(3));
            release();
        })
        .detach();
}

void FaultSimulator::simulateDisconnect()
{
    triggered.store(true, std::memory_order_release);

    std::thread(
        [this]
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            release();
        })
        .detach();
}

void FaultSimulator::release()
{
    triggered.store(false, std::memory_order_release);
}

void FaultSimulator::setCrashCallback(std::function<void()> callback)
{
    crashCallback = std::move(callback);
}

std::optional<FaultAction> FaultInjectionContext::evalAction(std::string_view failpointName)
{
    // TODO if crashed, stop triggering new failpoints
    /*if (simulator.check())
    {
        return std::nullopt;
    }*/
    auto str = std::string(failpointName);
    auto it = rules.find(str);
    if (it == rules.end())
    {
        return std::nullopt;
    }

    hitCountStats[std::string(failpointName)].fetch_add(1, std::memory_order_relaxed);

    FaultAction action = FaultAction::NONE;
    for (auto& rule : it->second)
    {
        if (rule.trigger->eval())
        {
            rule.trigger->triggered.store(true, std::memory_order::relaxed);
            if (action != FaultAction::NONE)
            {
                NES_WARNING("failpoint triggered multiple conflicting actions");
            }
            action = rule.action;
        }
    }

    if (action == FaultAction::NONE)
    {
        return std::nullopt;
    }
    return action;
}

void FaultInjectionContext::applyAction(FaultAction action)
{
    switch (action)
    {
        case FaultAction::CRASH:
            simulator.simulateCrash();
            break;
        case FaultAction::DISCONNECT:
            simulator.simulateDisconnect();
            break;
        case FaultAction::UDF:
            break;
        case FaultAction::NONE:
            break;
    }
}

bool FaultInjectionContext::evalFailpoint(std::string_view failpointName)
{
    auto action = evalAction(failpointName);
    if (!action.has_value())
    {
        return false;
    }
    applyAction(*action);
    return true;
}

std::vector<std::string> FaultInjectionContext::pendingFailpoints()
{
    std::vector<std::string> pending;
    for (auto& [name, fpRules] : rules)
    {
        for (auto& rule : fpRules)
        {
            if (!rule.trigger->triggered)
            {
                pending.push_back(name);
                break;
            }
        }
    }
    return pending;
}

void FaultInjectionContext::configure(std::string configYaml)
{
    rules.clear();
    hitCountStats.clear();
    parseConfigAndInitFaultContext(configYaml, *this);
}

thread_local FaultInjectionContext* currentFaultContext = nullptr;

FaultInjectionContext* getActiveFaultContext()
{
    return currentFaultContext;
}

void initActiveFaultContext(Host host)
{
    currentFaultContext = FaultContextRegistry::instance().getFaultContext(host);
}

FaultContextRegistry& FaultContextRegistry::instance()
{
    static FaultContextRegistry registry;
    return registry;
}

FaultInjectionContext* FaultContextRegistry::getFaultContext(Host host)
{
    std::lock_guard lock(mutex);

    auto& context = faultContexts[host];
    if (!context)
    {
        context = std::make_unique<FaultInjectionContext>(host);
    }
    return context.get();
}

void FaultContextRegistry::configure()
{
}

}