
#include <API/Config.hpp>

namespace iotdb {

Config::Config() {
  preloading = false;
  measuring = false;
  parallelism = 1;
  bufferSize = 1;
  pipelinePermutation = 0;
}

Config Config::create() { return Config(); }

Config &Config::withHashTableSize(size_t hash_table_rows) {
  this->hash_table_size = hash_table_rows;
  return *this;
}

Config &Config::withBufferSize(unsigned int bufferSize) {
  this->bufferSize = bufferSize;
  return *this;
}

Config &Config::withNumberOfPassesOverInput(unsigned int num_passes) {
  this->number_of_passes_over_input_files = num_passes;
  return *this;
}

Config &Config::withParallelism(unsigned int parallelism) {
  this->parallelism = parallelism;
  return *this;
}

Config &Config::withPipelinePermutation(unsigned int pipelinePermutation) {
  this->pipelinePermutation = pipelinePermutation;
  return *this;
}

Config &Config::withMeasuring() {
  this->measuring = true;
  return *this;
}

Config &Config::withPreloading() {
  this->preloading = true;
  return *this;
}

bool Config::getMeasuring() { return measuring; }

bool Config::getPreLoading() { return preloading; }

std::string Config::getPreLoadingAsString() {
  if (preloading)
    return "true";
  else
    return "false";
}

size_t Config::getHashTableSize() { return hash_table_size; }

unsigned int Config::getBufferSize() { return bufferSize; }

unsigned int Config::getParallelism() { return parallelism; }

unsigned int Config::getNumberOfPassesOverInput() { return number_of_passes_over_input_files; }

unsigned int Config::getPipelinePermutation() { return pipelinePermutation; }
}
