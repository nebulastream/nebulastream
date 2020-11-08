#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_ExecutableNestedLoopJoinTriggerAction_HPP_
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <State/StateManager.hpp>
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
        }

        for (auto& leftHashTable : leftJoinState->rangeAll()) {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: leftHashTable" << toString() << " check key=" << leftHashTable.first << "nextEdge=" << leftHashTable.second->nextEdge);
            for (auto& rightHashTable : rightJoinSate->rangeAll()) {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: rightHashTable" << toString() << " check key=" << rightHashTable.first << "nextEdge=" << rightHashTable.second->nextEdge);
                {
                    if (leftHashTable.first == rightHashTable.first) {
                        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: found join pair");
                    }
                }
            }

            //
            //            // write all window aggregates to the tuple buffer
            //            aggregateWindows(it.first, it.second, tupleBuffer);//put key into this
            //            // TODO we currently have no handling in the case the tuple buffer is full
        }
        return true;
    }

    std::string toString() {
        return "ExecutableNestedLoopJoinTriggerAction";
    }

    uint64_t updateWaterMark(WindowSliceStore<KeyType>* store) {
        auto windowType = joinDefinition->getWindowType();
        auto windowTimeType = windowType->getTimeCharacteristic();
        auto watermark = windowTimeType->getType() == TimeCharacteristic::ProcessingTime ? getTsFromClock() : store->getMinWatermark();

        // the window type adds result windows to the windows vectors
        if (store->getLastWatermark() == 0) {
            if (windowType->isTumblingWindow()) {
                TumblingWindow* tumbWindow = dynamic_cast<TumblingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows: successful cast to TumblingWindow");
                auto initWatermark = watermark < tumbWindow->getSize().getTime() ? 0 : watermark - tumbWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows(TumblingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else if (windowType->isSlidingWindow()) {
                SlidingWindow* slidWindow = dynamic_cast<SlidingWindow*>(windowType.get());
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows: successful cast to SlidingWindow");
                auto initWatermark = watermark < slidWindow->getSize().getTime() ? 0 : watermark - slidWindow->getSize().getTime();
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows(SlidingWindow): getLastWatermark was 0 set to=" << initWatermark);
                store->setLastWatermark(initWatermark);
            } else {
                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows: Unknown WindowType; LastWatermark was 0 and remains 0");
            }
        } else {
            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows: last watermark is=" << store->getLastWatermark());
        }
        return watermark;
    }

    /**
  * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
  * which are written to the tuple buffer.
  * @param store
  * @param tupleBuffer
  */
    void aggregateWindows(KeyType key,
                          WindowSliceStore<KeyType>* store,
                          TupleBuffer& tupleBuffer) {

        NES_DEBUG("tupleBuffer=" << key << " " << store << " " << tupleBuffer);
        //        // For event time we use the maximal records ts as watermark.
        //        // For processing time we use the current wall clock as watermark.
        //        // TODO we should add a allowed lateness to support out of order events
        //        auto watermark = updateWaterMark(store);
        //
        //        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction::aggregateWindows: current watermark is=" << watermark << " minWatermark=" << store->getMinWatermark());
        //
        //        // create result vector of windows
        //
        //        //TODO this will be replaced by the the watermark operator
        //        // iterate over all slices and update the partial final aggregates
        //        auto slices = store->getSliceMetadata();
        //        auto partialAggregates = store->getPartialAggregates();
        //        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction: trigger " << slices.size() << " slices");
        //
        //        //if slice creator, find slices which can be send but did not send already
        //        NES_DEBUG("ExecutableNestedLoopJoinTriggerAction SL: trigger Slicing for " << slices.size() << " slices");
        //
        //        for (uint64_t sliceId = 0; sliceId < slices.size(); sliceId++) {
        //            //test if latest tuple in window is after slice end
        //            NES_DEBUG("ExecutableNestedLoopJoinTriggerAction SL:  << slices[sliceId].getStartTs()=" << slices[sliceId].getStartTs() << "slices[sliceId].getEndTs()=" << slices[sliceId].getEndTs() << " watermark=" << watermark << " sliceID=" << sliceId);
        //            if (slices[sliceId].getEndTs() <= watermark) {
        //                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction SL: write result");
        //                writeResultRecord<PartialAggregateType>(tupleBuffer, tupleBuffer.getNumberOfTuples(),
        //                                                        slices[sliceId].getStartTs(),
        //                                                        slices[sliceId].getEndTs(),
        //                                                        key,
        //                                                        partialAggregates[sliceId]);
        //                tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
        //                store->removeSlicesUntil(sliceId);
        //            } else {
        //                NES_DEBUG("ExecutableNestedLoopJoinTriggerAction SL: Dont write result");
        //            }
        //        }
        //
        //        store->setLastWatermark(watermark);
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
