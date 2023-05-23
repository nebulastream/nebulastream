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
#include <Execution/StatisticsCollector/Normalizer.hpp>
#include <future>
#include <iostream>
#include <numeric>

namespace NES::Runtime::Execution {

Normalizer::Normalizer(uint64_t windowSize, std::unique_ptr<ChangeDetector> changeDetector) :
        windowSize(windowSize), window(0), changeDetector(std::move(changeDetector)), max(0) {}

bool Normalizer::normalizeValue(uint64_t value){
    std::lock_guard<std::mutex> lock(changeDetector->mutex);
    if (window.size() < windowSize) {
        window.push_back(value);

        if (window.size() == windowSize) {
            max = *max_element(window.begin(), window.end());
            auto change = std::async(std::launch::async, &Normalizer::addNormalizedValuesToChangeDetection, this);
            return change.get();
        }
        return false;
    }

    double normalizedValue = (double) value /(double) max;
    if (normalizedValue >= 1){
        changeDetector->reset();
        max = value;
        normalizedValue = (double) value /(double) max;
    }

    return changeDetector->insertValue(normalizedValue);
}

bool Normalizer::addNormalizedValuesToChangeDetection(){
    bool change = false;
    for (auto value : window) {
        auto normalizedValue = (double) value / (double) max ;
        if (changeDetector->insertValue(normalizedValue)) {
            change = true;
        }
    }
    return change;
}

} // namespace NES::Runtime::Execution