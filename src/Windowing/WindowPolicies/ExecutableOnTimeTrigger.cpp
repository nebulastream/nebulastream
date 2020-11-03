#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTrigger.hpp>
#include <memory>

namespace NES::Windowing {

bool ExecutableOnTimeTrigger::start(AbstractWindowHandlerPtr windowHandler) {
    std::unique_lock lock(runningTriggerMutex);
    if (running) {
        return false;
    }
    running = true;
    std::string triggerType;
    if (windowHandler->getWindowDefinition()->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowHandler->getWindowDefinition()->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
        triggerType = "Combining";
    } else {
        triggerType = "Slicing";
    }

    NES_DEBUG("ExecutableOnTimeTrigger started thread " << this << " handler=" << windowHandler << " type=" << triggerType << " with ms=" << triggerTimeInMs);
    std::string handlerName = windowHandler->toString();
    thread = std::make_shared<std::thread>([handlerName, windowHandler, this]() {
        setThreadName("whdlr-%d", handlerName.c_str());
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(triggerTimeInMs));
            windowHandler->trigger();
        }
    });
    return true;
}

bool ExecutableOnTimeTrigger::stop() {
    std::unique_lock lock(runningTriggerMutex);
    NES_DEBUG("ExecutableOnTimeTrigger " << this << ": Stop called");
    if (!running) {
        NES_DEBUG("ExecutableOnTimeTrigger " << this << ": Stop called but was already not running");
        return false;
    }
    running = false;

    if (thread && thread->joinable()) {
        thread->join();
    }
    thread.reset();
    NES_DEBUG("ExecutableOnTimeTrigger " << this << ": Thread joinded");
    // TODO what happens to the content of the window that it is still in the state?
    return true;
}

ExecutableOnTimeTrigger::ExecutableOnTimeTrigger(size_t triggerTimeInMs) : triggerTimeInMs(triggerTimeInMs), running(false), runningTriggerMutex() {
}

ExecutableOnTimeTriggerPtr ExecutableOnTimeTrigger::create(size_t triggerTimeInMs) {
    return std::make_shared<ExecutableOnTimeTrigger>(triggerTimeInMs);
}

}// namespace NES::Windowing
