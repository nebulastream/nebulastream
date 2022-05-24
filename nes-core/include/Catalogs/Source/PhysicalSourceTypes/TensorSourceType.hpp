//
// Created by eleicha on 20.05.22.
//

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TENSORSOURCETYPE_HPP
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TENSORSOURCETYPE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES {

class TensorSourceType;
using TensorSourceTypePtr = std::shared_ptr<TensorSourceType>;

class TensorSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static TensorSourceTypePtr create(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief create a MQTTSourceConfigPtr object
     * @param sourceConfigMap inputted config options
     * @return MQTTSourceConfigPtr
     */
    static TensorSourceTypePtr create(Yaml::Node yamlConfig);

    /**
     * @brief create a MQTTSourceConfigPtr object with default values
     * @return MQTTSourceConfigPtr
     */
    static TensorSourceTypePtr create();

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief gets a ConfigurationOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setSkipHeader(bool skipHeader);

    /**
     * @brief gets a ConfigurationOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getDelimiter() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setDelimiter(std::string delimiter);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

    /**
     * @brief gets a ConfigurationOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigurationOption object with numberOfTuplesToProducePerBuffer
     */
    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

  private:
    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit TensorSourceType(std::map<std::string, std::string> sourceConfigMap);

    /**
     * @brief constructor to create a new MQTT source config object initialized with values from sourceConfigMap
     */
    explicit TensorSourceType(Yaml::Node yamlConfig);

    /**
     * @brief constructor to create a new MQTT source config object initialized with default values as set below
     */
    TensorSourceType();

    Configurations::StringConfigOption filePath;
    Configurations::BoolConfigOption skipHeader;
    Configurations::StringConfigOption delimiter;
    Configurations::StringConfigOption inputFormat;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption numberOfTuplesToProducePerBuffer;

};

}
#endif//NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_TENSORSOURCETYPE_HPP
