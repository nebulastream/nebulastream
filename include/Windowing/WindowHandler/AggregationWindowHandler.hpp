#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTrigger.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerDescription.hpp>
#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class AggregationWindowHandler : public AbstractWindowHandler {
  public:
    explicit AggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition,
                                      std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation, ExecutableOnTimeTriggerPtr executablePolicyTrigger)
        : windowDefinition(std::move(windowDefinition)), windowAggregation(std::move(windowAggregation)), executablePolicyTrigger(std::move(executablePolicyTrigger)) {
        windowTupleSchema = Schema::create()
                                ->addField(createField("start", UINT64))
                                ->addField(createField("end", UINT64))
                                ->addField("key", this->windowDefinition->getOnKey()->getStamp())
                                ->addField("value", this->windowDefinition->getWindowAggregation()->as()->getStamp());
        windowTupleLayout = createRowLayout(windowTupleSchema);
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
        std::string triggerType;
        if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
            triggerType = "Combining";
        } else {
            triggerType = "Slicing";
        }

        // create the output tuple buffer
        // TODO can we make it get the buffer only once?
        auto tupleBuffer = bufferManager->getBufferBlocking();
        NES_DEBUG("AggregationWindowHandler: check widow trigger " << triggerType << " origin id=" << originId);

        tupleBuffer.setOriginId(originId);

        // iterate over all keys in the window state
        for (auto& it : windowStateVariable->rangeAll()) {
            NES_DEBUG("AggregationWindowHandler: " << triggerType << " check key=" << it.first << "nextEdge=" << it.second->nextEdge);

            // write all window aggregates to the tuple buffer
            aggregateWindows(it.first, it.second, this->windowDefinition, tupleBuffer);//put key into this
            // TODO we currently have no handling in the case the tuple buffer is full
        }
        // if produced tuple then send the tuple buffer to the next pipeline stage or sink
        if (tupleBuffer.getNumberOfTuples() > 0) {
            NES_DEBUG("AggregationWindowHandler: " << triggerType << " Dispatch output buffer with " << tupleBuffer.getNumberOfTuples() << " records, content="
                                                   << UtilityFunctions::prettyPrintTupleBuffer(tupleBuffer, windowTupleSchema)
                                                   << " originId=" << tupleBuffer.getOriginId() << "type=" << triggerType
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
        NES_DEBUG("AggregationWindowHandler::aggregateWindows: current watermark is=" << watermark << " minWatermark=" << store->getMinWatermark());

        // create result vector of windows
        auto windows = std::vector<WindowState>();
        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("AggregationWindowHandler::aggregateWindows: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("AggregationWindowHandler::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("AggregationWindowHandler::aggregateWindows: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("AggregationWindowHandler::aggregateWindows(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("AggregationWindowHandler::aggregateWindows: Unknown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("AggregationWindowHandler::aggregateWindows: last watermark is=" << store->getLastWatermark());
        }

        // iterate over all slices and update the partial final aggregates
        auto slices = store->getSliceMetadata();
        auto partialAggregates = store->getPartialAggregates();
        NES_DEBUG("AggregationWindowHandler: trigger " << windows.size() << " windows, on " << slices.size() << " slices");

        //trigger a central window operator
        if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                NES_DEBUG("AggregationWindowHandler: trigger sliceid=" << sliceId << " start=" << slices[sliceId].getStartTs() << " end=" << slices[sliceId].getEndTs());
            }
            //generates a list of windows that have to be outputted

            NES_DEBUG("AggregationWindowHandler: trigger test if mappings=" << store->getNumberOfMappings() << " < inputEdges=" << windowDefinition->getNumberOfInputEdges());
            if (store->getNumberOfMappings() < windowDefinition->getNumberOfInputEdges()) {
                NES_DEBUG("AggregationWindowHandler: trigger cause only " << store->getNumberOfMappings() << " mappings we set watermark to last=" << store->getLastWatermark());
                watermark = store->getLastWatermark();
            }
            windowDefinition->getWindowType()->triggerWindows(windows, store->getLastWatermark(), watermark);//watermark
            NES_DEBUG("AggregationWindowHandler: trigger Complete or combining window for slices=" << slices.size() << " windows=" << windows.size());

            // allocate partial final aggregates for each window
            //because we trigger each second, there could be multiple windows ready
            auto partialFinalAggregates = std::vector<PartialAggregateType>(windows.size());
            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                    auto window = windows[windowId];
                    // A slice is contained in a window if the window starts before the slice and ends after the slice
                    NES_DEBUG("AggregationWindowHandler CC: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs()
                                                                                  << " window.getEndTs()=" << window.getEndTs() << " slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs());
                    if (window.getStartTs() <= slices[sliceId].getStartTs() && window.getEndTs() >= slices[sliceId].getEndTs()) {
                        NES_DEBUG("AggregationWindowHandler CC: create partial agg windowId=" << windowId << " sliceId=" << sliceId);
                        partialFinalAggregates[windowId] = windowAggregation->combine(partialFinalAggregates[windowId], partialAggregates[sliceId]);
                    } else {
                        NES_DEBUG("AggregationWindowHandler CC: condition not true");
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
            NES_DEBUG("AggregationWindowHandler SL: trigger Slicing for " << slices.size() << " slices");

            for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
                //test if latest tuple in window is after slice end
                NES_DEBUG("AggregationWindowHandler SL:  << slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs() << "slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs() << " watermark=" << watermark << " sliceID=" << sliceId);
                if (slices[sliceId].getEndTs() <= watermark) {
                    NES_DEBUG("AggregationWindowHandler SL: write result");
                    writeResultRecord<PartialAggregateType>(tupleBuffer, tupleBuffer.getNumberOfTuples(),
                                                            slices[sliceId].getStartTs(),
                                                            slices[sliceId].getEndTs(),
                                                            key,
                                                            partialAggregates[sliceId]);
                    tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
                    store->removeSlicesUntil(sliceId);
                } else {
                    NES_DEBUG("AggregationWindowHandler SL: Dont write result");
                }
            }
        } else {
            NES_ERROR("Window combiner not implemented yet");
            NES_NOT_IMPLEMENTED();
        }
        store->setLastWatermark(watermark);
    }

    /**
     * @brief Writes a value to the output buffer with the following schema
     * -- start_ts, end_ts, key, value --
     * @tparam ValueType Type of the particular value
     * @param tupleBuffer reference to the tuple buffer we want to write to
     * @param index record index
     * @param startTs start ts of the window/slice
     * @param endTs end ts of the window/slice
     * @param key key of the value
     * @param value value
     */
    template<typename ValueType>
    void writeResultRecord(TupleBuffer& tupleBuffer, uint64_t index, uint64_t startTs, uint64_t endTs, KeyType key, ValueType value) {
        windowTupleLayout->getValueField<uint64_t>(index, 0)->write(tupleBuffer, startTs);
        windowTupleLayout->getValueField<uint64_t>(index, 1)->write(tupleBuffer, endTs);
        windowTupleLayout->getValueField<KeyType>(index, 2)->write(tupleBuffer, key);
        windowTupleLayout->getValueField<ValueType>(index, 3)->write(tupleBuffer, value);
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

  private:
    LogicalWindowDefinitionPtr windowDefinition;

  private:
    StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable;
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation;
    ExecutableOnTimeTriggerPtr executablePolicyTrigger;
    MemoryLayoutPtr windowTupleLayout;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
