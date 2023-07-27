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

  CountMin::CountMin(double error, double prob, const std::string& physicalSourceName, const std::string& field, time_t duration, time_t interval)
      : Sketch((ceil(log(1.0 / prob))), (ceil(M_E/error)), physicalSourceName, field, duration, interval) {

    this->setClassOfHashFunctions(new H3(this->getDepth(), this->getWidth()));

    uint32_t** sketchPointer;
    sketchPointer = (uint32_t**)malloc(sizeof(uint32_t*) * this->getDepth());

    for (uint32_t i = 0; i < this->getDepth(); i++) {
      sketchPointer[i] = (uint32_t*)malloc(sizeof(uint32_t) * this->getWidth());
    }

    memset(sketchPointer[0], 0, this->getDepth() * this->getWidth() * sizeof(uint32_t));

    this->setDataPointer(sketchPointer);

    std::cout << "Sketch created" << std::endl;
  }

  CountMin* CountMin::createCountMinWidthDepth(uint32_t depth,
                                               uint32_t width,
                                               Yaml::Node configNode) {

    std::string physicalSourceName = configNode["physicalSourceName"].As<std::string>();
    std::string field = configNode["field"].As<std::string>();
    auto key = configNode["key"].As<uint32_t>();
    auto duration = std::stoi(configNode["duration"].As<std::string>());
    auto frequency = std::stoi(configNode["frequency"].As<std::string>());

    double error = 1 / (std::exp(depth));
    double prob = std::exp(1 / width);

    auto sketch = new CountMin(error, prob, physicalSourceName, field, duration, frequency);

    return sketch;
  }

  CountMin* CountMin::createCountMinErrorProb(double error, double prob, Yaml::Node configNode) {

    std::string physicalSourceName = configNode["physicalSourceName"].As<std::string>();
    std::string field = configNode["field"].As<std::string>();
    auto key = configNode["key"].As<uint32_t>();
    auto duration = std::stoi(configNode["duration"].As<std::string>());
    auto frequency = std::stoi(configNode["frequency"].As<std::string>());

    auto sketch = new CountMin(error, prob, physicalSourceName, field, duration, frequency);

    return sketch;
  }

/*  CountMin* CountMin::initialize(*//*const std::string& physicalSourceName,
      const std::string& field,
      time_t duration,
      time_t interval,*//*
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

      std::cout << "Trying to create a sketch" << std::endl;
      mySketchA = CountMin::createCountMinErrorProb(error, prob, physicalSourceName, field, duration, interval);
    } else {
      std::cout << "trying to create a CM Sketch with too many parametrized arguments!" << std::endl;
      std::cout << "Only pass either an error and a probability or a depth and a width!" << std::endl;
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

  bool CountMin::equal(StatCollector* otherSketch) {

    CountMin* leftCM;
    leftCM = dynamic_cast<CountMin*>(this);
    CountMin* rightCM;
    rightCM = dynamic_cast<CountMin*>(otherSketch);

    if (leftCM->getWidth() != rightCM->getWidth() || leftCM->getDepth() != rightCM->getDepth()) {
      return false;
    }

    auto leftQ = leftCM->getClassOfHashFunctions()->getQ();
    auto rightQ = rightCM->getClassOfHashFunctions()->getQ();

    for (uint32_t i = 0; i < this->getDepth() * sizeof(uint32_t) * 8; i++) {
      if (leftQ[i] != rightQ[i]) {
        return false;
      }
    }
    return true;
  }

  StatCollector* CountMin::merge(StatCollector* rightSketch) {

    if (!(this->equal(rightSketch))){
      std::cout << "Cannot merge the two sketches, as they are not equal" << std::endl;
    }

    CountMin* leftCM;
    leftCM = dynamic_cast<CountMin*>(this);

    CountMin* rightCM;
    rightCM = dynamic_cast<CountMin*>(rightSketch);

    double_t error = leftCM->getError();
    double_t prob = leftCM->getProb();

    auto mergedCM = new CountMin(error, prob, "mergedSources", this->getField(), this->getDuration(), this->getFrequency());

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
      uint32_t index = hashFunctions->hashH3(key, q + 32 * i);
      if (sketch[i][index] < min) {
        min = sketch[i][index];
      }
    }
    return min;
  }
} // NES