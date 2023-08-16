#ifndef NES_TEXTDATASET_HPP
#define NES_TEXTDATASET_HPP

/*
enum DataSet { IPSUM_WORDS, IPSUM_SENTENCES };

class TextDataSet {
  public:
    TextDataSet() = default;

    static std::string getFilePath(DataSet dataSet) {
        switch (dataSet) {
            case IPSUM_WORDS:
                return "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_words.txt";
            case IPSUM_SENTENCES:
                return "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_sentences.txt";
        }
    }
    static constexpr size_t getMaxIndexValue(DataSet dataSet) {
        switch (dataSet) {
            case IPSUM_WORDS: return 185;
            case IPSUM_SENTENCES: return 1713;
        }
    }
    static constexpr size_t getMaxValueLength(DataSet dataSet) {
        switch (dataSet) {
            case IPSUM_WORDS: return 13;
            case IPSUM_SENTENCES: return 177;
        }
    }
};
*/

enum DataSet { IPSUM_WORDS, IPSUM_SENTENCES };

class TextDataSet {
  public:
    static const std::string filePath;
    static constexpr size_t maxIndexValue{};
    static constexpr size_t maxValueLength{};
};

class IpsumWords : public TextDataSet {
  public:
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_words.txt";
    static constexpr size_t maxIndexValue = 185;
    static constexpr size_t maxValueLength = 13;
};

class IpsumSentences : public TextDataSet {
  public:
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_sentences.txt";
    static constexpr size_t maxIndexValue = 1713;
    static constexpr size_t maxValueLength = 177;
};

#endif//NES_TEXTDATASET_HPP
