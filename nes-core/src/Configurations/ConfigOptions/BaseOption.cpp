#include <Configurations/ConfigOptions/BaseOption.hpp>

namespace NES::Configurations {

BaseOption::BaseOption(std::string name, std::string description) : name(name), description(description){};

bool BaseOption::operator==(const BaseOption& other) { return name == other.name && description == other.description; };

std::string BaseOption::getName() { return name; }

std::string BaseOption::getDescription() { return description; }

}// namespace NES::Configurations