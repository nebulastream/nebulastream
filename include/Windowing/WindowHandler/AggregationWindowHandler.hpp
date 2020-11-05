#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class AggregationWindowHandler : public AbstractWindowHandler {
  public:
    explicit AggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition,
                                      std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation,
                                      ExecutableOnTimeTriggerPtr executablePolicyTrigger,
                                      BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction)
        : windowDefinition(std::move(windowDefinition)), executableWindowAggregation(std::move(windowAggregation)), executablePolicyTrigger(std::move(executablePolicyTrigger)), executableWindowAction(std::move(executableWindowAction)) {
    }

    ~AggregationWindowHandler() {
        NES_DEBUG("AggregationWindowHandler: calling destructor");
        stop();
    }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override {
        return executablePolicyTrigger->start(this->shared_from_this());
    }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        return executablePolicyTrigger->stop();
    }

    std::string toString() override {
        std::stringstream ss;
        ss << pipelineStageId << +"-" << nextPipeline->getQepParentId();
        return ss.str();
    }

    /**
     * @brief triggers all ready windows.
     * @return
     */
    void trigger() override {
        NES_DEBUG("AggregationWindowHandler: run window action " << executableWindowAction->toString()
                                                                 << " distribution type=" << windowDefinition->getDistributionType()->toString()
                                                                 << " origin id=" << originId);
        auto tupleBuffer = bufferManager->getBufferBlocking();
        tupleBuffer.setOriginId(originId);
        executableWindowAction->doAction(getTypedWindowState(), tupleBuffer);

        if (tupleBuffer.getNumberOfTuples() > 0) {
            NES_DEBUG("AggregationWindowHandler: Dispatch output buffer with " << tupleBuffer.getNumberOfTuples() << " records, content="
                                                                               << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, executableWindowAction->getWindowSchema())
                                                                               << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << executableWindowAction->toString()
                                                                               << std::endl);
            //forward buffer to next pipeline stage
            queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);
        } else {
            NES_WARNING("AggregationWindowHandler: output buffer size is 0 and therefore now buffer is forwarded");
        }
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    void updateAllMaxTs(uint64_t ts, uint64_t originId) override {
        //TODO: check if we still need this
        NES_DEBUG("AggregationWindowHandler: updateAllMaxTs with ts=" << ts << " originId=" << originId);
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("AggregationWindowHandler: update ts for key=" << it.first << " store=" << it.second << " maxts=" << it.second->getMaxTs(originId) << " nextEdge=" << it.second->nextEdge);
            it.second->updateMaxTs(ts, originId);
        }
    }

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline, uint32_t pipelineStageId, uint64_t originId) override {
        this->queryManager = queryManager;
        this->bufferManager = bufferManager;
        this->pipelineStageId = pipelineStageId;
        this->originId = originId;

        // Initialize AggregationWindowHandler Manager
        this->windowManager = std::make_shared<WindowManager>(this->windowDefinition);
        // Initialize StateVariable
        this->windowStateVariable = &StateManager::instance().registerState<KeyType, WindowSliceStore<PartialAggregateType>*>("window");
        this->nextPipeline = nextPipeline;

        NES_ASSERT(!!this->nextPipeline, "Error on pipeline");
        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    WindowManagerPtr getWindowManager() override { return this->windowManager; }

    auto getTypedWindowState() {
        return windowStateVariable;
    }

    LogicalWindowDefinitionPtr getWindowDefinition() override {
        return windowDefinition;
    }

    LogicalWindowDefinitionPtr windowDefinition;

  private:
    StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable;
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    ExecutableOnTimeTriggerPtr executablePolicyTrigger;
    BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
