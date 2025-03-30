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

#include <Sinks/Mediums/MultiOriginWatermarkProcessor.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Windowing {

MultiOriginWatermarkProcessor::MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins) : origins(origins) {
    for (const auto& _ : origins) {
        watermarkProcessors.emplace_back(std::make_shared<Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>());
    }
};

std::shared_ptr<MultiOriginWatermarkProcessor> MultiOriginWatermarkProcessor::create(const std::vector<OriginId>& origins) {
    return std::make_shared<MultiOriginWatermarkProcessor>(origins);
}

// TODO use here the BufferMetaData class for the params #4177
uint64_t MultiOriginWatermarkProcessor::updateWatermark(uint64_t ts, SequenceData sequenceData, OriginId origin) {
    bool found = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        if (origins[originIndex] == origin) {
            watermarkProcessors[originIndex]->emplace(sequenceData, ts);
            found = true;
        }
    }
    if (!found) {
        std::stringstream ss;
        for (auto& id : origins) {
            ss << id << ",";
        }
        NES_THROW_RUNTIME_ERROR("update watermark for non existing origin " << origin << " number of origins=" << origins.size()
                                                                            << " ids=" << ss.str());
    }
    return getCurrentWatermark();
}

std::string MultiOriginWatermarkProcessor::getCurrentStatus() {
    std::stringstream ss;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex) {
        ss << " id=" << origins[originIndex] << " watermark=" << watermarkProcessors[originIndex]->getCurrentValue();
    }
    return ss.str();
}

uint64_t MultiOriginWatermarkProcessor::getCurrentWatermark() {
    auto minimalWatermark = UINT64_MAX;
    for (const auto& wt : watermarkProcessors) {
        minimalWatermark = std::min(minimalWatermark, wt->getCurrentValue());
    }
    return minimalWatermark;
}

}// namespace NES::Windowing
