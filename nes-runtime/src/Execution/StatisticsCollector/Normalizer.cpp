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

Normalizer::Normalizer(uint64_t windowSize, std::unique_ptr<ChangeDetectorWrapper> changeDetectorWrapper) :
        windowSize(windowSize), window(0), changeDetectorWrapper(std::move(changeDetectorWrapper)), max(0) {}

void Normalizer::normalizeValue(uint64_t value){
    if (window.size() < windowSize) {
        window.push_back(value);
        if (window.size() == windowSize) {
            max = *max_element(window.begin(), window.end());
            auto a = std::async(std::launch::async, &Normalizer::addNormalizedValuesToChangeDetection, this);
        }
        return ;
    }

    double normalizedValue = (double) value /(double) max;
    if (changeDetectorWrapper->insertValue(normalizedValue)){
        std::cout << "Change detected" << std::endl;
    }
}

void Normalizer::addNormalizedValuesToChangeDetection(){
    for (auto value : window) {
        auto normalizedValue = (double) value / (double) max ;
        if (changeDetectorWrapper->insertValue(normalizedValue)) {
            std::cout << "Change detected" << std::endl;
        }
    }
}

} // namespace NES::Runtime::Execution