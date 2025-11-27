#pragma once
#include <string>
#include <sstream>

inline std::string makeTopologyName(uint64_t queryId) {
    std::ostringstream ss; ss << "file-" << queryId;
    return ss.str();
}

inline std::string makeQueryName(uint64_t queryId) {
    std::ostringstream ss; ss << "query-" << queryId;
    return ss.str();
}

inline std::string makeSinkName(const std::string& testName, uint64_t queryId) {
    std::ostringstream ss; ss << testName << "-" << queryId << "-sink";
    return ss.str();
}

inline std::string makeType(const std::string& baseType, uint64_t queryId) {
    std::ostringstream ss; ss << baseType << "-" << queryId;
    return ss.str();
}
