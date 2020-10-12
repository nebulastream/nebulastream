#include <Windowing/Runtime/WindowManager.hpp>

namespace NES {

WindowManager::WindowManager(WindowDefinitionPtr windowDefinition)
    : windowDefinition(std::move(windowDefinition)), allowedLateness(0) {}

WindowDefinitionPtr WindowManager::getWindowDefinition() {
    return windowDefinition;
}

uint64_t WindowManager::getAllowedLateness() const {
    return allowedLateness;
}

}// namespace NES