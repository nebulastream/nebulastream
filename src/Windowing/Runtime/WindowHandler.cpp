#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <atomic>
#include <utility>

#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES::Windowing {

WindowHandler::WindowHandler(LogicalWindowDefinitionPtr windowDefinition)
    : std::enable_shared_from_this<WindowHandler>(), windowDefinition(std::move(windowDefinition)), originId(0) {
    this->thread.reset();
    windowTupleSchema = Schema::create()
                            ->addField(createField("start", UINT64))
                            ->addField(createField("end", UINT64))
                            ->addField("key", this->windowDefinition->getOnKey()->getStamp())
                            ->addField("value", this->windowDefinition->getWindowAggregation()->as()->getStamp());
    windowTupleLayout = createRowLayout(windowTupleSchema);
}


uint64_t WindowHandler::getOriginId() const {
    return originId;
}

void WindowHandler::setOriginId(uint64_t originId) {
    this->originId = originId;
}

}// namespace NES
