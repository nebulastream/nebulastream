#include <Windowing/WindowActions/ExecutableWindowAggregationTriggerAction.hpp>

namespace NES::Windowing {
bool ExecutableWindowAggregationTriggerAction::doAction() {
    return true;
}

ExecutableWindowAggregationTriggerAction::ExecutableWindowAggregationTriggerAction() {
}

BaseExecutableWindowActionPtr ExecutableWindowAggregationTriggerAction::create() {
    return std::make_shared<ExecutableWindowAggregationTriggerAction>();
}

}// namespace NES::Windowing