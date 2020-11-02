#include <Windowing/WindowPolicies/ExecutableOnTimeTrigger.hpp>
#include <Util/ThreadNaming.hpp>
#include <memory>
#include <Util/Logger.hpp>

namespace NES::Windowing {

bool ExecutableOnTimeTrigger::start(AbstractWindowHandlerPtr windowHandler) {
    std::unique_lock lock(runningTriggerMutex);
    if (running) {
        return false;
    }
    running = true;
    NES_DEBUG("AggregationWindowHandler " << this << " handler=" << windowHandler << ": Spawn thread");
    thread = std::make_shared<std::thread>([&]() {
      setThreadName("whdlr-%d", windowHandler->getHandlerName());
      std::this_thread::sleep_for(std::chrono::milliseconds(triggerTimeInMs));
      windowHandler->trigger();

    });
    return true;
}

bool ExecutableOnTimeTrigger::stop() {
    std::unique_lock lock(runningTriggerMutex);
    NES_DEBUG("AggregationWindowHandler " << this << ": Stop called");
    if (!running) {
        NES_DEBUG("AggregationWindowHandler " << this << ": Stop called but was already not running");
        return false;
    }
    running = false;

    if (thread && thread->joinable()) {
        thread->join();
    }
    thread.reset();
    NES_DEBUG("AggregationWindowHandler " << this << ": Thread joinded");
    // TODO what happens to the content of the window that it is still in the state?
    return true;
}

ExecutableOnTimeTrigger::ExecutableOnTimeTrigger(size_t triggerTimeInMs): triggerTimeInMs(triggerTimeInMs)
{
}

ExecutableOnTimeTriggerPtr ExecutableOnTimeTrigger::create(size_t triggerTimeInMs)
{
    return std::make_shared<ExecutableOnTimeTrigger>(triggerTimeInMs);
}

}