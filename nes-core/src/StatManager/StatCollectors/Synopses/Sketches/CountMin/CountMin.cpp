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

#include <cstring>
#include <cmath>
#include <Util/Logger/Logger.hpp>
#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp"

namespace NES::Experimental::Statistics {

  double CountMin::getError() const {
    return mError;
  }
  double CountMin::getProb() const {
    return mProb;
  }

  uint32_t** CountMin::getDataPointer() {
    return mDataPointer;
  }

  void CountMin::setDataPointer(uint32_t** dataPointer) {
    mDataPointer = dataPointer;
  }

  H3* CountMin::getClassOfHashFunctions(){
    return mClassOfHashingFunctions;
  }

  void CountMin::setClassOfHashFunctions(H3* ClassOfHashingFunctions){
    mClassOfHashingFunctions = ClassOfHashingFunctions;
  }

  CountMin::CountMin(const StatCollectorConfig& config)
      : Sketch(config,
               ((uint32_t) ceil(log(1.0 / static_cast<double_t >(config.getProbability())))),
               ((uint32_t) ceil(M_E/static_cast<double_t >(config.getError())))), mError(config.getError()),
               mProb(config.getProbability()), mDataPointer(nullptr), mClassOfHashingFunctions(nullptr) {

    this->setClassOfHashFunctions(new H3(this->getDepth(), this->getWidth()));

    uint32_t** sketchPointer;
    sketchPointer = (uint32_t**)malloc(sizeof(uint32_t*) * this->getDepth());

    for (uint32_t i = 0; i < this->getDepth(); i++) {
      sketchPointer[i] = (uint32_t*)malloc(sizeof(uint32_t) * this->getWidth());
    }

    memset(sketchPointer[0], 0, this->getDepth() * this->getWidth() * sizeof(uint32_t));

    this->setDataPointer(sketchPointer);

    NES_DEBUG("Sketch created");
  }

  void CountMin::update(uint32_t key) {

    H3* hashFunctions = this->getClassOfHashFunctions();
    uint32_t* q = hashFunctions->getQ();

    uint32_t** sketchPointer = CountMin::getDataPointer();
    uint32_t hash = 0;
    for (uint32_t i = 0; i < this->getDepth(); i++) {
      hash = hashFunctions->hashH3(key, q + (i * 32)) % this->getWidth();
      sketchPointer[i][hash] += 1;
    }
  }

  bool CountMin::equal(const std::unique_ptr<StatCollector>& rightSketch, bool statCollection) {

    auto leftCM = dynamic_cast<CountMin*>(this);
    auto rightCM = dynamic_cast<CountMin*>(rightSketch.get());

    // if the sketches don't have the same dimensions / (error/prob) guarantees, then they aren't equal
    if (leftCM->getWidth() != rightCM->getWidth() || leftCM->getDepth() != rightCM->getDepth()) {
      return false;
    }

    // two sketches are only similar/equal, if they are built on fields that contain equal data
    if (leftCM->getField() != rightCM->getField()) {
      return false;
    }

    // if we are checking whether two sketches are equal within the context of statistics collection, then their duration and frequency also needs to be identical
    if (statCollection) {
      if ((leftCM->getDuration() != rightCM->getDuration()) || (leftCM->getFrequency() != rightCM->getFrequency())) {
        return false;
      }
    }

    // the hash-functions need to be initialized to the same seeds
    auto leftQ = leftCM->getClassOfHashFunctions()->getQ();
    auto rightQ = rightCM->getClassOfHashFunctions()->getQ();

    for (uint32_t i = 0; i < this->getDepth() * sizeof(uint32_t) * 8; i++) {
      if (leftQ[i] != rightQ[i]) {
        return false;
      }
    }
    return true;
  }

  std::unique_ptr<StatCollector> CountMin::merge(std::unique_ptr<StatCollector> rightSketch, bool statCollection) {

    if (!(this->equal(rightSketch, statCollection))) {
      NES_DEBUG("Cannot merge the two sketches, as they are not equal");
      return nullptr;
    }

    auto leftCM = dynamic_cast<CountMin*>(this);
    auto rightCM = dynamic_cast<CountMin*>(rightSketch.get());

    if (leftCM == nullptr || rightCM == nullptr) {
      NES_DEBUG("Cast of StatCollector to CountMin Sketch was not possible!");
      return nullptr;
    }

    double_t error = leftCM->getError();
    double_t prob = leftCM->getProb();

    // TODO: come up with a better way for this;
    // combinedStreamName
    auto PSN1 = leftCM->getPhysicalSourceName();
    auto PSN2 = rightCM->getPhysicalSourceName();
    auto cmSketchConfig = new StatCollectorConfig(PSN1 + PSN2, leftCM->getField(), "FrequencyCM", leftCM->getDuration(),
                                     leftCM->getFrequency(), error, prob, 0, 0);

    CountMinPtr mergedSketch = std::make_unique<CountMin>(*cmSketchConfig);
//    CountMinPtr mergedSketch = std::make_unique<CountMin>(*cmSketchConfig);

    uint32_t** leftSketchData = leftCM->getDataPointer();
    uint32_t** rightSketchData = rightCM->getDataPointer();
    uint32_t** mergedSketchData = mergedSketch->getDataPointer();

    for (uint32_t i = 0; i < leftCM->getDepth(); i++) {
      for (uint32_t j = 0; j < leftCM->getWidth(); j++) {
        mergedSketchData[i][j] = leftSketchData[i][j] + rightSketchData[i][j];
      }
    }

    return std::move(mergedSketch);
  }

  uint32_t CountMin::pointQuery(uint32_t key) {

    uint32_t min = UINT32_MAX;

    uint32_t** sketch = this->getDataPointer();

    auto hashFunctions = this->getClassOfHashFunctions();
    auto q = hashFunctions->getQ();

    for (uint32_t i = 0; i < this->getDepth(); i++){
      uint32_t index = hashFunctions->hashH3(key, q + 32 * i) % this->getWidth();
      if (sketch[i][index] < min) {
        min = sketch[i][index];
      }
    }
    return min;
  }
} // NES::Experimental::Statistics