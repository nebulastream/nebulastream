
#include <string>

namespace iotdb{


class Config {
public:
  static Config create();
  size_t getHashTableSize();
  Config &withHashTableSize(size_t hash_table_rows);
  unsigned int getParallelism();
  Config &withParallelism(unsigned int parallelism);
  unsigned int getBufferSize();
  Config &withBufferSize(unsigned int bufferSize);
  unsigned int getNumberOfPassesOverInput();
  Config &withNumberOfPassesOverInput(unsigned int num_passes);
  unsigned int getPipelinePermutation();
  Config &withPipelinePermutation(unsigned int pipelinePermuation);
  bool getMeasuring();
  Config &withMeasuring();
  bool getPreLoading();
  Config &withPreloading();
  std::string getPreLoadingAsString();

private:
  Config();
  size_t hash_table_size;
  unsigned int parallelism;
  unsigned int bufferSize;
  unsigned int pipelinePermutation;
  unsigned int number_of_passes_over_input_files;
  bool measuring;
  bool preloading;
};

}
