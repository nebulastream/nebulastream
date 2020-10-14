#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERIMPL_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERIMPL_HPP_

#include <State/StateManager.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/DistributionCharacteristic.hpp>

#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>

#include <QueryCompiler/PipelineStage.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/ThreadNaming.hpp>
namespace NES {

/**
 * @brief This represents a window during query execution.
 */
template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class WindowHandlerImpl : public WindowHandler {
  public:
    WindowHandlerImpl() = default;
    WindowHandlerImpl(LogicalWindowDefinitionPtr windowDefinition,
                      std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation) : WindowHandler(windowDefinition), windowAggregation(windowAggregation){};

    bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline, uint32_t pipelineStageId) override {
        this->queryManager = queryManager;
        this->bufferManager = bufferManager;
        this->pipelineStageId = pipelineStageId;
        this->thread.reset();
        // Initialize WindowHandler Manager
        this->windowManager = std::make_shared<WindowManager>(this->windowDefinition);
        // Initialize StateVariable
        this->windowStateVariable = &StateManager::instance().registerState<KeyType, WindowSliceStore<PartialAggregateType>*>("window");
        this->nextPipeline = nextPipeline;
        NES_ASSERT(!!this->nextPipeline, "Error on pipeline");
        return true;
    }

    bool start() override {
        std::unique_lock lock(runningTriggerMutex);
        if (running) {
            return false;
        }
        running = true;
        NES_DEBUG("WindowHandler " << this << ": Spawn thread");
        thread = std::make_shared<std::thread>([this]() {
            setThreadName("whdlr-%d-%d", pipelineStageId, nextPipeline->getQepParentId());
            trigger();
        });
        return true;
    }

    bool stop() override {
        std::unique_lock lock(runningTriggerMutex);
        NES_DEBUG("WindowHandler " << this << ": Stop called");
        if (!running) {
            NES_DEBUG("WindowHandler " << this << ": Stop called but was already not running");
            return false;
        }
        running = false;

        if (thread && thread->joinable()) {
            thread->join();
        }
        thread.reset();
        NES_DEBUG("WindowHandler " << this << ": Thread joinded");
        // TODO what happens to the content of the window that it is still in the state?
        return true;
    }

    void updateAllMaxTs(uint64_t ts, uint64_t originId) override {
        NES_DEBUG("WindowHandler: updateAllMaxTs with ts=" << ts << " originId=" << originId);
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("WindowHandler: update ts for key=" << it.first << " store=" << it.second << " maxts=" << it.second->getMaxTs(originId) << " nextEdge=" << it.second->nextEdge);
            it.second->updateMaxTs(ts, originId);
        }
    }

    /**
     * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
     * which are written to the tuple buffer.
     * @param store
     * @param windowDefinition
     * @param tupleBuffer
     */
    void aggregateWindows(KeyType key,
                          WindowSliceStore<PartialAggregateType>* store,
                          LogicalWindowDefinitionPtr windowDefinition,
                          TupleBuffer& tupleBuffer) {
        // For event time we use the maximal records ts as watermark.
        // For processing time we use the current wall clock as watermark.
        // TODO we should add a allowed lateness to support out of order events
        auto windowType = windowDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();
        NES_DEBUG("WindowHandler::aggregateWindows: current watermark is=" << watermark << " minWatermark=" << store->getMinWatermark());

        // create result vector of windows
        auto windows = std::vector<WindowState>();
        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("WindowHandler::aggregateWindows: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("WindowHandler::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("WindowHandler::aggregateWindows: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("WindowHandler::aggregateWindows(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("WindowHandler::aggregateWindows: Unkown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("WindowHandler::aggregateWindows: last watermark is=" << store->getLastWatermark());
        }

        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();
        NES_DEBUG("WindowHandler: trigger " << windows.size() << " windows, on " << slices.size() << " slices");

        //trigger a central window operator
        if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                NES_DEBUG("WindowHandler: trigger sliceid=" << sliceId << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
            }
            //generates a list of windows that have to be outputted

            NES_DEBUG("WindowHandler: trigger test if mappings=" << store->getNumberOfMappings() << " < inputEdges=" << windowDefinition->getNumberOfInputEdges());
            if (store->getNumberOfMappings() < windowDefinition->getNumberOfInputEdges()) {
                NES_DEBUG("WindowHandler: trigger cause only " << store->getNumberOfMappings() << " mappings we set watermark to last=" << store->getLastWatermark());
                watermark = store->getLastWatermark();
            }
            windowDefinition->getWindowType()->triggerWindows(windows, store->getLastWatermark(), watermark);//watermark
            NES_DEBUG("WindowHandler: trigger Complete or combining window for slices=" << slices.size() << " windows=" << windows.size());

            // allocate partial final aggregates for each window
            //because we trigger each second, there could be multiple windows ready
            auto partialFinalAggregates = std::vector<PartialAggregateType>(windows.size());
            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                    auto window = windows[windowId];
                    // A slice is contained in a window if the window starts before the slice and ends after the slice
                    NES_DEBUG("WindowHandler CC: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs()
                                                                       << " window.getEndTs()=" << window.getEndTs() << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs());
                    if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()) {
                        NES_DEBUG("WindowHandler CC: create partial agg windowId=" << windowId << " sliceId=" << sliceId);

                        partialFinalAggregates[windowId] = windowAggregation->combine(partialFinalAggregates[windowId], partialAggregates[sliceId]);

                    } else {
                        NES_DEBUG("WindowHandler CC: condition not true");
                    }
                }
            }
            // calculate the final aggregate
            for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
                auto window = windows[i];
                auto value = windowAggregation->lower(partialFinalAggregates[i]);
                NES_DEBUG("Window Handler: write key=" << key << " value=" << value << " window.start()="
                                                       << window.getStartTs() << " window.getEndTs()="
                                                       << window.getEndTs() << " tupleBuffer.getNumberOfTuples())" << tupleBuffer.getNumberOfTuples());

                writeResultRecord<FinalAggregateType>(tupleBuffer,
                                                      tupleBuffer.getNumberOfTuples(),
                                                      window.getStartTs(),
                                                      window.getEndTs(),
                                                      key,
                                                      value);

                //TODO: we have to determine which windwos and keys to delete
                tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
            }
            //TODO: remove content from state

        } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Slicing) {
            //if slice creator, find slices which can be send but did not send already
            NES_DEBUG("WindowHandler SL: trigger Slicing for " << slices.size() << " slices");

            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                //test if latest tuple in window is after slice end
                NES_DEBUG("WindowHandler SL:  << slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs() << "slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs() << " watermark=" << watermark << " sliceID=" << sliceId);
                if (slices[sliceId].getEndTs() <= watermark) {
                    NES_DEBUG("WindowHandler SL: write result");
                    writeResultRecord<PartialAggregateType>(tupleBuffer, tupleBuffer.getNumberOfTuples(),
                                                            slices[sliceId].getStartTs(),
                                                            slices[sliceId].getEndTs(),
                                                            key,
                                                            partialAggregates[sliceId]);
                    tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
                    store->removeSlicesUntil(sliceId);
                } else {
                    NES_DEBUG("WindowHandler SL: Dont write result");
                }
            }
        } else {
            NES_ERROR("Window combiner not implemented yet");
            NES_NOT_IMPLEMENTED();
        }

        store->setLastWatermark(watermark);
    };

    void trigger() override {
        while (running) {
            sleep(1);

            NES_DEBUG("WindowHandler: check widow trigger origin id=" << originId);
            /*
            std::string triggerType;
            if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
                triggerType = "Combining";
            } else {
                triggerType = "Slicing";
            }

            // create the output tuple buffer
            // TODO can we make it get the buffer only once?
            auto tupleBuffer = bufferManager->getBufferBlocking();
            NES_DEBUG("WindowHandler: check widow trigger " << triggerType << " origin id=" << originId);

            tupleBuffer.setOriginId(originId);
            // iterate over all keys in the window state
            for (auto& it : windowStateVariable->rangeAll()) {
                NES_DEBUG("WindowHandler: " << triggerType << " check key=" << it.first << "nextEdge=" << it.second->nextEdge);

                // write all window aggregates to the tuple buffer
                aggregateWindows(it.first, it.second, this->windowDefinition, tupleBuffer);//put key into this
                // TODO we currently have no handling in the case the tuple buffer is full
            }
            // if produced tuple then send the tuple buffer to the next pipeline stage or sink
            if (tupleBuffer.getNumberOfTuples() > 0) {
                NES_DEBUG("WindowHandler: " << triggerType << " Dispatch output buffer with " << tupleBuffer.getNumberOfTuples() << " records, content="
                                            << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, windowTupleSchema)
                                            << " originId=" << tupleBuffer.getOriginId() << "type=" << triggerType
                                            << std::endl);
                queryManager->addWorkForNextPipeline(
                    tupleBuffer,
                    this->nextPipeline);
            }
             */
        }
    }

    template<typename ValueType>
    void writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key, ValueType value) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
        windowTupleLayout->getValueField<ValueType>(index, 3)->write(tupleBuffer, value);
    }

    void* getWindowState() override {
        return windowStateVariable;
    }

    ~WindowHandlerImpl() {
        NES_DEBUG("WindowHandler: calling destructor");
        stop();
    }

  private:
    StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable;
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation;
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERIMPL_HPP_
