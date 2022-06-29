#ifndef NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_JSONSOURCETYPE_HPP_
#define NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_JSONSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>

namespace NES {

enum JSONFormat {
    JSON,  // https://datatracker.ietf.org/doc/html/rfc8259
    NDJSON,// https://github.com/ndjson/ndjson-spec
    JSONL  // https://jsonlines.org/
};

class JSONSourceType;
using JSONSourceTypePtr = std::shared_ptr<JSONSourceType>;
/**
 * @brief Configuration object for JSON source config
 * define configurations for a JSON source, i.e. this source reads from data from a JSON file
 */
class JSONSourceType : public PhysicalSourceType {

  public:
    ~JSONSourceType() noexcept override = default;

    /**
     * @brief create a JSONSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return JSONSourceTypePtr
     */
    static JSONSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a JSONSourceTypePtr object
     * @param sourceConfigMap inputted config options
     * @return JSONSourceTypePtr
     */
    static JSONSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a default JSONSourceTypePtr object
     * @return JSONSourceTypePtr
     */
    static JSONSourceTypePtr create();

    /**
     * @brief creates a string representation of the source
     * @return
     */
    std::string toString() override;

    /**
     * Checks equality
     * @param other physicalSourceType ot check equality for
     * @return true if equal, false otherwise
     */
    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    /**
     * @brief Get file path, needed for JSONSource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief Get number of buffers to read
     * @see nes-core/include/Sources/DataSource.hpp
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumBuffersToProcess() const;

    /**
     * @brief Set file path, needed for JSONSource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief Set number of buffers to read
     * @see nes-core/include/Sources/DataSource.hpp
     */
    void setNumBuffersToProcess(uint32_t numBuffersToProcess);

  private:
    /**
     * @brief constructor to create a new JSON source config object initialized with values from sourceConfigMap
     */
    explicit JSONSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new JSON source config object initialized with values from sourceConfigMap
     */
    explicit JSONSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new JSON source config object initialized with default values
     */
    JSONSourceType();
    Configurations::StringConfigOption filePath;
    Configurations::IntConfigOption numBuffersToProcess;
};
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_JSONSOURCETYPE_HPP_
