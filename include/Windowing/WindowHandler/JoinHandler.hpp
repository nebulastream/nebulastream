#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
#include <State/StateManager.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType>
class JoinHandler : public Windowing::AbstractWindowHandler {
  public:
    explicit JoinHandler(LogicalJoinDefinitionPtr joinDefinition,
                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                         BaseExecutableJoinActionPtr<KeyType> executableJoinAction) : joinDefinition(joinDefinition),
                                                                                      executablePolicyTrigger(executablePolicyTrigger),
                                                                                      executableJoinAction(executableJoinAction) {
        NES_DEBUG("Construct JoinHandler");
    }

    ~JoinHandler() {
        NES_DEBUG("JoinHandler: calling destructor");
        stop();
    }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override {
        NES_DEBUG("JoinHandler start");
        return executablePolicyTrigger->start(this->shared_from_this());
    }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        NES_DEBUG("JoinHandler stop");
        return executablePolicyTrigger->stop();
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "AG:" << pipelineStageId << +"-" << nextPipeline->getQepParentId();
        return ss.str();
    }

    /**
    * @brief triggers all ready windows.
    */
    void trigger() override {
        NES_DEBUG("JoinHandler: run window action " << executableJoinAction->toString()
                                                    << " origin id=" << originId);
        auto tupleBuffer = bufferManager->getBufferBlocking();
        tupleBuffer.setOriginId(originId);
        executableJoinAction->doAction(leftJoinState, rightJoinState, tupleBuffer);

        if (tupleBuffer.getNumberOfTuples() > 0) {
            NES_DEBUG("JoinHandler: Dispatch output buffer with " << tupleBuffer.getNumberOfTuples() << " records, content="
                                                                  << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, executableJoinAction->getJoinSchema())
                                                                  << " originId=" << tupleBuffer.getOriginId() << "windowAction=" << executableJoinAction->toString()
                                                                  << std::endl);
            //forward buffer to next pipeline stage
            queryManager->addWorkForNextPipeline(tupleBuffer, this->nextPipeline);
        } else {
            NES_WARNING("JoinHandler: output buffer size is 0 and therefore now buffer is forwarded");
        }
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    void updateAllMaxTs(uint64_t ts, uint64_t originId) override {
        NES_DEBUG("JoinHandler: updateAllMaxTs with ts=" << ts << " originId=" << originId);
        for (auto& it : leftJoinState->rangeAll()) {
            NES_DEBUG("JoinHandler left: update ts for key=" << it.first << " store=" << it.second << " maxts=" << it.second->getMaxTs(originId) << " nextEdge=" << it.second->nextEdge);
            it.second->updateMaxTs(ts, originId);
            NES_DEBUG("JoinHandler left: max is=" << it.second->getMaxTs(originId));
        }
        for (auto& it : rightJoinState->rangeAll()) {
            NES_DEBUG("JoinHandler right: update ts for key=" << it.first << " store=" << it.second << " maxts=" << it.second->getMaxTs(originId) << " nextEdge=" << it.second->nextEdge);
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

        // Initialize JoinHandler Manager
        this->windowManager = std::make_shared<Windowing::WindowManager>(joinDefinition->getWindowType());
        // Initialize StateVariable
        this->leftJoinState = &StateManager::instance().registerState<KeyType, WindowSliceStore<KeyType>*>("leftSide");
        this->rightJoinState = &StateManager::instance().registerState<KeyType, WindowSliceStore<KeyType>*>("rightSide");
        this->nextPipeline = nextPipeline;

        NES_ASSERT(!!this->nextPipeline, "Error on pipeline");
        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    Windowing::WindowManagerPtr getWindowManager() override {
        return this->windowManager;
    }

    auto getLeftJoinState() {
        return leftJoinState;
    }

    auto getRightJoinState() {
        return rightJoinState;
    }
    LogicalJoinDefinitionPtr getJoinDefinition() {
        return joinDefinition;
    }

    LogicalWindowDefinitionPtr getWindowDefinition() override {
        return nullptr;
    }

  private:
    StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState;
    StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinState;

    //TODO: this will activated once we have a slice store that is capable of handling vectors
    //    StateVariable<KeyType, Windowing::WindowSliceStore<std::vector<ValueTypeLeft>>*>* leftJoinState;
    //    StateVariable<KeyType, Windowing::WindowSliceStore<std::vector<ValueTypeRight>>*>* rightJoinState;

    LogicalJoinDefinitionPtr joinDefinition;
    Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    Join::BaseExecutableJoinActionPtr<KeyType> executableJoinAction;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
