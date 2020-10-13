#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <utility>
#include <zconf.h>

namespace NES {

WindowHandler::WindowHandler(NES::LogicalWindowDefinitionPtr windowDefinitionPtr)
    : windowDefinition(std::move(windowDefinitionPtr)), originId(0) {
    this->thread.reset();
    windowTupleSchema = Schema::create()
                            ->addField(createField("start", UINT64))
                            ->addField(createField("end", UINT64))
                            ->addField(createField("key", INT64))
                            ->addField("value", INT64);
    windowTupleLayout = createRowLayout(windowTupleSchema);
}


uint64_t WindowHandler::getOriginId() const {
    return originId;
}

void WindowHandler::setOriginId(uint64_t originId) {
    this->originId = originId;
}

}// namespace NES
