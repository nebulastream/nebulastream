#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType>
class ExecutableNestedLoopJoinTriggerAction : public BaseExecutableJoinAction<KeyType> {
  public:
    static ExecutableNestedLoopJoinTriggerActionPtr<KeyType> create(LogicalJoinDefinitionPtr joinDefintion) {
        return std::make_shared<Join::ExecutableNestedLoopJoinTriggerAction<KeyType>>(joinDefintion);
    }

    ExecutableNestedLoopJoinTriggerAction(LogicalJoinDefinitionPtr joinDefinition) : joinDefinition(joinDefinition) {
        windowSchema = Schema::create()
                           ->addField(createField("start", UINT64))
                           ->addField(createField("end", UINT64))
                           ->addField("key", this->joinDefinition->getJoinKey()->getStamp())
                           ->addField("value", this->joinDefinition->getJoinKey()->getStamp());
        windowTupleLayout = createRowLayout(windowSchema);
    }

    bool doAction(StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState,
                  StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinSate,
                  TupleBuffer& tupleBuffer) {
        // iterate over all keys in the window state
        NES_DEBUG("tupleBuffer=" << tupleBuffer);

        NES_DEBUG("leftJoinState content=");
        for (auto& [key, val] : leftJoinState->rangeAll()) {
            NES_DEBUG("Key: " << key << " Value: " << val);
            for (auto& slice : val->getSliceMetadata()) {
                NES_DEBUG("start=" << slice.getStartTs() << " end=" << slice.getEndTs());
            }
            for (auto agg : val->getPartialAggregates()) {
                NES_DEBUG("key=" << key);
                NES_DEBUG("value=" << agg);
            }
            NES_DEBUG(" last watermark=" << val->getLastWatermark() << " minwatermark=" << val->getMinWatermark() << " allMaxTs=" << val->getAllMaxTs());
        }

        NES_DEBUG("rightJoinSate content=");
        for (auto& [key, val] : rightJoinSate->rangeAll()) {
            NES_DEBUG("Key: " << key << " Value: " << val);
            for (auto& slice : val->getSliceMetadata()) {
                NES_DEBUG("start=" << slice.getStartTs() << " end=" << slice.getEndTs());
            }
            for (auto agg : val->getPartialAggregates()) {
                NES_DEBUG("key=" << key);
                NES_DEBUG("value=" << agg);
            }
            NES_DEBUG(" last watermark=" << val->getLastWatermark() << " minwatermark=" << val->getMinWatermark() << " allMaxTs=" << val->getAllMaxTs());
        }

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: leftHashTable" << toString() << " check key=" << leftHashTable.first << " nextEdge=" << leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: rightHashTable" << toString() << " check key=" << rightHashTable.first << " nextEdge=" << rightHashTable.second->nextEdge);
                {
                    if (leftHashTable.first == rightHashTable.first) {

                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: found join pair for key " << leftHashTable.first);
                        joinWindows(leftHashTable.first, leftHashTable.second, rightHashTable.second, tupleBuffer);
                    }
                }
            }
        }

        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction doing the nested loop join");
        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : leftHashTable.second->getMinWatermark();
            leftHashTable.second->setLastWatermark(watermark);
        }
        for (auto& rightHashTable : rightJoinSate->rangeAll()) {
            auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : rightHashTable.second->getMinWatermark();
            rightHashTable.second->setLastWatermark(watermark);
        }

        return true;
    }

    std::string toString() {
        return "ExecutableNestedLoopJoinTriggerAction";
    }

    uint64_t getWatermark(WindowSliceStore<KeyType>* store) {
        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();

        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: Unknown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::updateWaterMark: last watermark is=" << store->getLastWatermark() << " for watermark=" << watermark);
        }
        return watermark;
    }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void joinWindows(KeyType key,
                     WindowSliceStore<KeyType>* leftStore,
                     WindowSliceStore<KeyType>* rightStore,
                     TupleBuffer& tupleBuffer) {

        // TODO we should add a allowed lateness to support out of order events
        auto watermarkLeft = getWatermark(leftStore);
        auto watermarkRight = getWatermark(rightStore);

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows:leftStore  watermark is=" << watermarkLeft << " minWatermark=" << leftStore->getMinWatermark() << " lastwatermark=" << leftStore->getLastWatermark());
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows:rightStore  watermark is=" << watermarkRight << " minWatermark=" << rightStore->getMinWatermark() << " lastwatermark=" << rightStore->getLastWatermark());

        // create result vector of windows, note that both sides will have the same windows
        auto windows = std::vector<WindowState>();

        //do the partial aggregation on the left side
        auto slicesLeft = leftStore->getSliceMetadata();
        auto partialAggregatesLeft = leftStore->getPartialAggregates();
        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger " << windows.size() << " windows, on " << slicesLeft.size() << " slices");
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger sliceid=" << sliceId << " start=" << slicesLeft[sliceId].getStartTs() << " end=" << slicesLeft[sliceId].getEndTs());
        }

        auto slicesRight = rightStore->getSliceMetadata();
        auto partialAggregatesRight = rightStore->getPartialAggregates();

        joinDefinition->getWindowType()->triggerWindows(windows, leftStore->getLastWatermark(), watermarkLeft);//watermark

        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction:leftStore trigger Complete or combining window for slices=" << slicesLeft.size() << " windows=" << windows.size());
        // allocate partial final aggregates for each window because we trigger each second, there could be multiple windows ready
        auto partialFinalAggregates = std::vector<KeyType>(windows.size());
        for (uint64_t sliceId = 0; sliceId < slicesLeft.size(); sliceId++) {
            for (uint64_t windowId = 0; windowId < windows.size(); windowId++) {
                auto window = windows[windowId];
                // A slice is contained in a window if the window starts before the slice and ends after the slice
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: window.getStartTs()=" << window.getStartTs() << " slices[sliceId].getStartTs()=" << slicesLeft[sliceId].getStartTs()
                                                                                        << " window.getEndTs()=" << window.getEndTs() << " slices[sliceId].getEndTs()=" << slicesLeft[sliceId].getEndTs());
                if (window.getStartTs() <= slicesLeft[sliceId].getStartTs() && window.getEndTs() >= slicesLeft[sliceId].getEndTs()) {
                    NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: create partial agg windowId=" << windowId << " sliceId=" << sliceId);
                    partialFinalAggregates[windowId] = partialFinalAggregates[windowId] + partialAggregatesLeft[sliceId] + partialAggregatesRight[sliceId];
                } else {
                    NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: condition not true");
                }
            }
        }

        // calculate the final aggregate
        for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
            auto window = windows[i];
            auto value = partialFinalAggregates[i];
            NES_DEBUG("Join Handler: write key=" << key << " value=" << value << " window.start()="
                                                 << window.getStartTs() << " window.getEndTs()="
                                                 << window.getEndTs() << " tupleBuffer.getNumberOfTuples())" << tupleBuffer.getNumberOfTuples());

            writeResultRecord<KeyType>(tupleBuffer,
                                       tupleBuffer.getNumberOfTuples(),
                                       window.getStartTs(),
                                       window.getEndTs(),
                                       key,
                                       value);

            //TODO: we have to determine which windows and keys to delete
            tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
        }
        //TODO: remove content from state

        //        leftStore->setLastWatermark(watermarkLeft);
        //        rightStore->setLastWatermark(watermarkRight);
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

    std::string getActionResultAsString(TupleBuffer& buffer) {
        return UtilityFunctions::prettyPrintTupleBuffer(buffer, windowSchema);
    }

    SchemaPtr getJoinSchema() {
        return windowSchema;
    }

  private:
    LogicalJoinDefinitionPtr joinDefinition;
    SchemaPtr windowSchema;
    MemoryLayoutPtr windowTupleLayout;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
