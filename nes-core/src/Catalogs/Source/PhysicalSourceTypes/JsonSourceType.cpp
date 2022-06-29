#include <Catalogs/Source/PhysicalSourceTypes/JSONSourceType.hpp>

namespace NES {

JSONSourceTypePtr JSONSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<JSONSourceType>(JSONSourceType(std::move(sourceConfigMap)));
}

JSONSourceTypePtr JSONSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<JSONSourceType>(JSONSourceType(std::move(yamlConfig)));
}

JSONSourceTypePtr JSONSourceType::create() { return std::make_shared<JSONSourceType>(JSONSourceType()); }

JSONSourceType::JSONSourceType(std::map<std::string, std::string> sourceConfigMap) : JSONSourceType() {
    NES_INFO("JSONSourceType: Init default JSON source config object with values from command line.");
    if (sourceConfigMap.find(Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("JSONSourceType: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
}

JSONSourceType::JSONSourceType(Yaml::Node yamlConfig) : JSONSourceType() {
    // TODO
    (void) yamlConfig;
}

JSONSourceType::JSONSourceType()
    : PhysicalSourceType(JSON_SOURCE),
      filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                        "",
                                                                        "file path, needed for: JSONSource")),
      numBuffersToProcess(Configurations::ConfigurationOption<std::uint32_t>::create(Configurations::FILE_PATH_CONFIG,
                                                                                     0,
                                                                                     "file path, needed for: JSONSource")) {
    NES_INFO("JSONSourceTypeConfig: Init source config object with default values.");
}

bool JSONSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<JSONSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<JSONSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue();
}

std::string JSONSourceType::toString() {
    std::stringstream ss;
    ss << "JSONSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

Configurations::StringConfigOption JSONSourceType::getFilePath() const { return filePath; }
Configurations::IntConfigOption JSONSourceType::getNumBuffersToProcess() const { return numBuffersToProcess; }

void JSONSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void JSONSourceType::setNumBuffersToProcess(uint32_t numBuffersToProcessValue) {
    numBuffersToProcess->setValue(numBuffersToProcessValue);
}

void JSONSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setNumBuffersToProcess(numBuffersToProcess->getDefaultValue());
}

}// namespace NES