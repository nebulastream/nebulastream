//
// Created by ttekdogan on 28/07/25.
//

#ifndef CONFIG_YAML_CONFIGLOADER_HPP
#define CONFIG_YAML_CONFIGLOADER_HPP

#pragma once
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

enum class Pattern { Uniform, Poisson, Zipfian, Burst };

inline Pattern pattern_from_string(const std::string& s) {
    if (s == "uniform") return Pattern::Uniform;
    if (s == "poisson") return Pattern::Poisson;
    if (s == "zipfian") return Pattern::Zipfian;
    if (s == "burst")   return Pattern::Burst;
    throw std::runtime_error("Unknown pattern: " + s);
}

struct DataSource {
    std::string type{"csv"};
    std::filesystem::path file{};
    std::string encoding{"base64"};
};

struct Mqtt {
    std::string host{"localhost"};
    uint16_t    port{1883};
    std::string topic{};
    bool        enabled{false};
};

struct PatternParams {
    // Used by burst (optional)
    double fraction{0.9};
    double distribution{0.2};
};

struct ZipfParams {
    int   period_ms{1000};
    double s{1.4};
    bool stochastic{true};
};

struct Config {
    std::string host{"0.0.0.0"};
    Pattern pattern{Pattern::Uniform};
    PatternParams pattern_params{};
    ZipfParams zipf{};

    DataSource dataset{};
    std::chrono::seconds duration{60};

    // NEW semantics:
    // data_rate = tuples per period (NOT tuples/sec)
    int data_rate{1000};

    // NEW key:
    // period = length of the throughput period in milliseconds
    std::chrono::milliseconds period{1000};

    // tuples per send (also controls buckets per period internally)
    int chunk_size{10};

    std::vector<uint16_t> ports; // server ports
    std::optional<Mqtt> mqtt; // optional
};

Config load_config(const std::string& path);

#endif //CONFIG_YAML_CONFIGLOADER_HPP
