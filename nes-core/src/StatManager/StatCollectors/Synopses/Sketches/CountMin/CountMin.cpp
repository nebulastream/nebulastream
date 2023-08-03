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
#include "StatManager/StatCollectors/Synopses/Sketches/CountMin/CountMin.hpp"

namespace NES {

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

  CountMin::CountMin(const Configurations::StatManagerConfig& config)
      : Sketch(config,
               (ceil(log(1.0 / static_cast<double_t >(config.probability.getValue())))),
               (ceil(M_E/static_cast<double_t >(config.error.getValue())))) {

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

//  CountMin* CountMin::createCountMinWidthDepth(const Configurations::StatManagerConfig& config,
//                                               uint32_t depth,
//                                               uint32_t width) {
//
//    std::string physicalSourceName = configNode["physicalSourceName"].As<std::string>();
//    std::string field = configNode["field"].As<std::string>();
//    auto key = configNode["key"].As<uint32_t>();
//    auto duration = std::stoi(configNode["duration"].As<std::string>());
//    auto frequency = std::stoi(configNode["frequency"].As<std::string>());
//
//    double error = 1 / (std::exp(depth));
//    double prob = std::exp(1 / width);
//
//    auto sketch = new CountMin(error, prob, physicalSourceName, field, duration, frequency);
//
//    return sketch;
//  }
//
//  CountMin* CountMin::createCountMinErrorProb(const Configurations::StatManagerConfig& config,
//                                              double error,
//                                              double prob) {
//
//    std::string physicalSourceName = configNode["physicalSourceName"].As<std::string>();
//    std::string field = configNode["field"].As<std::string>();
//    auto key = configNode["key"].As<uint32_t>();
//    auto duration = std::stoi(configNode["duration"].As<std::string>());
//    auto frequency = std::stoi(configNode["frequency"].As<std::string>());
//
//    auto sketch = new CountMin(error, prob, physicalSourceName, field, duration, frequency);
//
//    return sketch;
//  }

/*  CountMin* CountMin::initialize(const std::string& physicalSourceName,
      const std::string& field,
      time_t duration,
      time_t interval,
      Yaml::Node configNode) {

    std::string physicalSourceName = configNode["physicalSourceName"].As<std::string>();
    std::string field = configNode["field"].As<std::string>();
    auto key = configNode["key"].As<uint32_t>();
    auto duration = std::stoi(configNode["duration"].As<std::string>());
    auto frequency = std::stoi(configNode["frequency"].As<std::string>());

    CountMin* mySketchA;
    if ((configNode["Attributes"][0]["Error"]).IsNone()
        && (configNode["Attributes"][0]["Prob"]).IsNone()
        && !((configNode["Attributes"][0]["Depth"]).IsNone())
        && !((configNode["Attributes"][0]["Width"]).IsNone())) {

      auto depth = (configNode["Attributes"][0]["Depth"]).As<uint32_t>();
      auto width = (configNode["Attributes"][0]["Width"]).As<uint32_t>();
      mySketchA = CountMin::createCountMinWidthDepth(depth, width, physicalSourceName, field, duration, interval);

    } else if (!((configNode["Attributes"][0]["Error"]).IsNone())
               && !((configNode["Attributes"][0]["Prob"]).IsNone())
               && (configNode["Attributes"][0]["Depth"]).IsNone()
               && (configNode["Attributes"][0]["Width"]).IsNone()) {

      auto error = (configNode["Attributes"][0]["Error"]).As<double_t>();
      auto prob = (configNode["Attributes"][0]["Prob"]).As<double_t>();

      mySketchA = CountMin::createCountMinErrorProb(error, prob, physicalSourceName, field, duration, interval);
    } else {
      NES_DEBUG("Could not  create a CM Sketch. Potentially too many parametrized arguments!");
    }
    return mySketchA;
  }*/

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

  bool CountMin::equal(StatCollector* otherSketch, bool statCollection) {

    CountMin* leftCM;
    leftCM = dynamic_cast<CountMin*>(this);
    CountMin* rightCM;
    rightCM = dynamic_cast<CountMin*>(otherSketch);

    // if the sketches don't have the same dimensions / (error/prob) guarantees, then they aren't equal
    if (leftCM->getWidth() != rightCM->getWidth() || leftCM->getDepth() != rightCM->getDepth()) {
      return false;
    }

    // two sketches are only similar/equal, if they are built on fields that contain equal data
    if (leftCM->getField() != rightCM->getField()) {
      return false;
    }

    // if we are checking whether two sketches are equal within the context of statistics collection, then their duration and frequency also needs to be identical
    if (statCollection == true) {
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

  StatCollector* CountMin::merge(StatCollector* rightSketch, bool statCollection) {

    if (!(this->equal(rightSketch, statCollection))){
      NES_DEBUG("Cannot merge the two sketches, as they are not equal");
      return nullptr;
    }

    CountMin* leftCM;
    leftCM = dynamic_cast<CountMin*>(this);

    CountMin* rightCM;
    rightCM = dynamic_cast<CountMin*>(rightSketch);

    double_t error = leftCM->getError();
    double_t prob = leftCM->getProb();

    auto cmSketchConfig = new Configurations::StatManagerConfig();

    // TODO: come up with a better way for this;
    // combinedStreamName
    auto PSN1 = leftCM->getPhysicalSourceName();
    auto PSN2 = rightCM->getPhysicalSourceName();

    cmSketchConfig->physicalSourceName.setValue(PSN1+PSN2);
    cmSketchConfig->field.setValue(leftCM->getField());
    cmSketchConfig->statMethodName.setValue("FrequencyCM");
    cmSketchConfig->duration.setValue(leftCM->getDuration());
    cmSketchConfig->frequency.setValue(leftCM->getFrequency());
    cmSketchConfig->error.setValue(error);
    cmSketchConfig->probability.setValue(prob);
    cmSketchConfig->depth.setValue(0);
    cmSketchConfig->width.setValue(0);

    auto mergedCM = new CountMin(*cmSketchConfig);

    uint32_t** leftSketchData = leftCM->getDataPointer();
    uint32_t** rightSketchData = rightCM->getDataPointer();
    uint32_t** mergedSketchData = mergedCM->getDataPointer();

    for (uint32_t i = 0; i < leftCM->getDepth(); i++) {
      for (uint32_t j = 0; j < leftCM->getDepth(); j++) {
        mergedSketchData[i][j] = leftSketchData[i][j] + rightSketchData[i][j];
      }
    }

    Sketch* mergedSketch = mergedCM;
    return mergedSketch;
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
} // NES