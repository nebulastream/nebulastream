#ifndef NES_TEXTDATASET_HPP
#define NES_TEXTDATASET_HPP

enum DataSet { INVALID = -1, IPSUM_WORDS, IPSUM_SENTENCES, TOP_500_DOMAINS, WIKI_TITLES_10000 };
NLOHMANN_JSON_SERIALIZE_ENUM(DataSet,
                             {{DataSet::INVALID, ""},
                              {DataSet::IPSUM_WORDS, "IPSUM_WORDS"},
                              {DataSet::IPSUM_SENTENCES, "IPSUM_SENTENCES"},
                              {DataSet::TOP_500_DOMAINS, "TOP_500_DOMAINS"},
                              {DataSet::WIKI_TITLES_10000, "WIKI_TITLES_10000"}})

class TextDataSet {
  public:
    explicit TextDataSet(DataSet dataSet) {
        this->dataSet = dataSet;
        switch (dataSet) {
            case INVALID: break;
            case IPSUM_WORDS: {
                this->filePath = "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_words.txt";
                this->maxIndexValue = 185;
                this->maxValueLength = 13;
                break;
            }
            case IPSUM_SENTENCES: {
                this->filePath =
                    "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_sentences.txt";
                this->maxIndexValue = 1713;
                this->maxValueLength = 177;
                break;
            }
            case TOP_500_DOMAINS:{
                this->filePath =
                    "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/top_500_domains_2.txt";
                this->maxIndexValue = 499;
                this->maxValueLength = 29;
                break;
            }
            case WIKI_TITLES_10000:{
                this->filePath =
                    "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/wikipedia_titles_10000.txt";
                this->maxIndexValue = 9999;
                this->maxValueLength = 227;
                break;
            }
        }
    }
    DataSet dataSet = DataSet::INVALID;
    std::string filePath;
    size_t maxIndexValue{};
    size_t maxValueLength{};
};
using TextDataSetPtr = std::shared_ptr<TextDataSet>;

// TODO remove below
class IpsumWords : public TextDataSet {
  public:
    static constexpr DataSet dataSet = DataSet::IPSUM_WORDS;
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_words.txt";
    static constexpr size_t maxIndexValue = 185;
    static constexpr size_t maxValueLength = 13;
};

class IpsumSentences : public TextDataSet {
  public:
    static constexpr DataSet dataSet = DataSet::IPSUM_SENTENCES;
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/ipsum_sentences.txt";
    static constexpr size_t maxIndexValue = 1713;
    static constexpr size_t maxValueLength = 177;
};

class Top500Urls : public TextDataSet {
  public:
    static constexpr DataSet dataSet = DataSet::TOP_500_DOMAINS;
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/top_500_domains_2.txt";
    static constexpr size_t maxIndexValue = 499;
    static constexpr size_t maxValueLength = 29;
};

class WikiTitles10000 : public TextDataSet {
  public:
    static constexpr DataSet dataSet = DataSet::WIKI_TITLES_10000;
    inline static const std::string filePath =
        "/home/maseiler/Coding/nebulastream/nes-benchmark/include/DataGeneration/data/wikipedia_titles_10000.txt";
    static constexpr size_t maxIndexValue = 9999;
    static constexpr size_t maxValueLength = 227;
};

#endif//NES_TEXTDATASET_HPP
