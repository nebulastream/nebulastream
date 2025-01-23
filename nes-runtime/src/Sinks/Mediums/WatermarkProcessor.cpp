/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Sinks/Mediums/WatermarkProcessor.hpp>

namespace NES::Windowing {

WatermarkProcessor::WatermarkProcessor() = default;

void WatermarkProcessor::updateWatermark(WatermarkTs ts, SequenceNumber sequenceNumber) {
    std::unique_lock lock(watermarkLatch);

    // emplace current watermark barrier in the update log
    transactionLog.emplace(ts, sequenceNumber);
    // process all outstanding updates from the queue
    while (!transactionLog.empty()) {
        auto nextWatermarkUpdate = transactionLog.top();
        // the update log is sorted by the sequence number.
        // Thus, we only check if the next update is the one, which we expect.
        // This implies, that each watermark barrier has to be received.
        // If the system looses a watermark barrier, the watermark processor will make no progress.
        if (currentSequenceNumber + 1 != std::get<1>(nextWatermarkUpdate)) {
            // It is not the correct update, so we terminate here and can't further apply the next transaction.
            break;
        }
        // apply the current update
        this->currentSequenceNumber = std::get<1>(nextWatermarkUpdate);
        this->currentWatermark = std::get<0>(nextWatermarkUpdate);
        transactionLog.pop();
    }
}

WatermarkTs WatermarkProcessor::getCurrentWatermark() const { return currentWatermark; }

bool WatermarkProcessor::isWatermarkSynchronized() const { return transactionLog.empty(); }

bool WatermarkProcessor::isDuplicate(SequenceNumber sequenceNumber) const {
    return currentSequenceNumber >= sequenceNumber;
}

}// namespace NES::Windowing
