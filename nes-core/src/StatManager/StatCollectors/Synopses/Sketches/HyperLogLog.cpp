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
#include <StatManager/StatCollectors/Synopses/Sketches/HyperLogLog.hpp>
#include <StatManager/Util/Util.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Experimental::Statistics {

  double HyperLogLog::getError() const {
    return mError;
  }

  uint16_t HyperLogLog::getB() const {
    return b;
  }

  double_t HyperLogLog::getAlphaM() const {
    return alphaM;
  }

  void HyperLogLog::setAlphaM(double_t alpha_m) {
    alphaM = alpha_m;
  }

  uint32_t HyperLogLog::getNumZeroBuckets() const {
    return numZeroBuckets;
  }

  uint32_t* HyperLogLog::getDataPointer() {
    return mDataPointer;
  }

  void HyperLogLog::setDataPointer(uint32_t* dataPointer) {
    mDataPointer = dataPointer;
  }

  H3* HyperLogLog::getClassOfHashFunctions(){
    return mClassOfHashingFunctions;
  }

  void HyperLogLog::setClassOfHashFunctions(H3* ClassOfHashingFunctions){
    mClassOfHashingFunctions = ClassOfHashingFunctions;
  }

  HyperLogLog::HyperLogLog(const StatCollectorConfig& config) : Sketch(config,
                                                                       1,
                                                                       roundUpToNextPowerOf2((uint32_t) ceil(1 / pow(config.getError(), 2)))){

    auto error = config.getError();
    uint32_t m = ceil(1 / pow(error, 2));
    m = roundUpToNextPowerOf2(m);

    if (m <= 16) {
      this->alphaM = 0.673;
    } else if (m == 32) {
      this->alphaM = 0.697;
    } else if (m == 64) {
      this->alphaM = 0.709;
      // if m >= 128
    } else {
      this->alphaM = 0.7213 / (1 + 0.079 / m);
    }

    this->mError = error;
    this->b = (log2(m));
    this->numZeroBuckets = m;

    uint32_t* sketchPointer = (uint32_t*)malloc(sizeof(uint32_t) * this->getWidth());

    memset(sketchPointer, 0, this->getWidth() * sizeof(uint32_t));

    this->setDataPointer(sketchPointer);
    this->setClassOfHashFunctions(new H3(this->getDepth(), this->getWidth()));

    NES_DEBUG("HyperLogLog Sketch created");
  }

  /*
  32 Bit update function
  // todo: 64 bit update function
  */
  void HyperLogLog::update(uint32_t key) {

    auto hashFunction = this->getClassOfHashFunctions();
    auto q = hashFunction->getQ();

    uint32_t* sketchPointer = this->getDataPointer();
    uint32_t hash = hashFunction->hashH3(key, q);

    uint32_t j = hash >> (32 - b);                                                // the first b bits
    uint32_t w = (hash << b) >> b;                                                // the other bits

    if ((uint32_t) __builtin_clz(w) + 1 > sketchPointer[j]) {

      if (sketchPointer[j] == 0) {
        this->numZeroBuckets -= 1;
      }

      sketchPointer[j] = __builtin_clz(w) + 1;
    }

    return;
  }

  bool HyperLogLog::equal(const std::unique_ptr<StatCollector>& otherSketch, bool statCollection) {

    auto leftHLL = dynamic_cast<HyperLogLog*>(this);
    auto rightHLL = dynamic_cast<HyperLogLog*>(otherSketch.get());

    // if the sketches have the same error, then they have the same m and b
    if (leftHLL->getError() != rightHLL->getError() || leftHLL->getAlphaM() != rightHLL->getAlphaM()) {
      return false;
    }

    // two sketches are only similar/equal, if they are built on fields that contain equal data
    if (leftHLL->getField() != rightHLL->getField()) {
      return false;
    }

    // if we are checking whether two sketches are equal within the context of statistics collection, then their duration and frequency also needs to be identical
    if (statCollection) {
      if ((leftHLL->getDuration() != rightHLL->getDuration()) || (leftHLL->getFrequency() != rightHLL->getFrequency())) {
        return false;
      }
    }

    // the hash-functions need to be initialized to the same seeds
    auto leftQ = leftHLL->getClassOfHashFunctions()->getQ();
    auto rightQ = rightHLL->getClassOfHashFunctions()->getQ();

    for (uint32_t i = 0; i < this->getDepth() * sizeof(uint32_t) * 8; i++) {
      if (leftQ[i] != rightQ[i]) {
        return false;
      }
    }
    return true;
  }

  std::unique_ptr<StatCollector> HyperLogLog::merge(std::unique_ptr<StatCollector>& rightSketch, bool statCollection) {

    if (!(this->equal(rightSketch, statCollection))){
      std::cout << "Cannot merge the two sketches, as they are not equal" << std::endl;
    }

    auto leftHLL = dynamic_cast<HyperLogLog*>(this);
    auto rightHLL = dynamic_cast<HyperLogLog*>(rightSketch.get());

    // TODO: come up with a better way for this;
    // combinedStreamName
    auto PSN1 = leftHLL->getPhysicalSourceName();
    auto PSN2 = rightHLL->getPhysicalSourceName();
    auto HLLConfig = new StatCollectorConfig(PSN1 + PSN2, leftHLL->getField(), "DistinctHLL", leftHLL->getDuration(),
                                                  leftHLL->getFrequency(), leftHLL->getError(), 0, 0, 0);

    HyperLogLogPtr mergedHLL = std::make_unique<HyperLogLog>(*HLLConfig);

    uint32_t* leftSketchData = leftHLL->getDataPointer();
    uint32_t* rightSketchData = rightHLL->getDataPointer();
    uint32_t* mergedSketchData = mergedHLL->getDataPointer();

    for (uint32_t i = 0; i < leftHLL->getWidth(); i++) {
      mergedSketchData[i] = std::max(leftSketchData[i], rightSketchData[i]);
    }

    return mergedHLL;
  }

  double_t HyperLogLog::countDistinctQuery() {

    auto sketch = this->getDataPointer();

    uint32_t sum = 0;

    for (uint32_t i = 0; i < this->getWidth(); i++) {
      sum += 2 << sketch[i];
    }


    auto a = pow(sum, -1);
    auto b = pow(this->getWidth(), 2);
    double_t E = this->getAlphaM() * a * b;

    if (E <= 2.5 * this->getWidth()) {
      if (this->numZeroBuckets != 0) {
        E = this->getWidth() * log((double_t) this->getWidth() / this->getNumZeroBuckets());
      }
    } else if (E > (1 / 30) * pow(2, 32)) {
      E = -1 * pow(2, 32) * log(1 - (E / pow(2,32)));
    }

    return E;
  }

}
