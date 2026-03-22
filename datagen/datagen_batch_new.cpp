#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <asio.hpp>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <chrono>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <charconv>
#include <cctype>
#include <cstring>
#include <algorithm>
#include <memory>
#include <mosquitto.h>
#include <random>
#include "include/yaml-cpp/yaml.h"
#include "Util/Yaml.h"

#include "configLoader.hpp"

using asio::ip::tcp;

// thread-safe queue for events
using MessageQueue = std::queue<std::string>;
static MessageQueue mqtt_queue;
static std::mutex queue_mutex;
static std::condition_variable queue_cv;

using AtomicBool = std::atomic<bool>;

// nanoseconds: 19
// microseconds: 16
// milliseconds: 13
constexpr int TS_WIDTH = 13;

inline int64_t now_nanos() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// this builds a batch buffer with placeholders for timestamps
// idx is advanced circularly through dataset.
struct SerializedRow {
    std::string prefix;
    std::string suffix;
};

struct Batch {
    std::string buf; // the serialized batch
    std::vector<size_t> ts_pos; // offsets in buf where timestamps must be written
};

static thread_local std::mt19937_64 rng{
    (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count()
};

Batch build_batch_with_placeholders(const std::vector<SerializedRow>& dataset,
                                    size_t& idx,
                                    int m)
{
    Batch b;
    b.ts_pos.reserve(m);

    // Rough reserve to avoid reallocations
    size_t reserve_bytes = 0;
    for (int i = 0, j = idx; i < m; ++i) {
        reserve_bytes += dataset[j].prefix.size() + 1 /*comma*/
                         + TS_WIDTH + dataset[j].suffix.size() + 1 /*newline*/;
        j = (j + 1) % dataset.size();
    }
    b.buf.reserve(reserve_bytes);

    for (int i = 0; i < m; ++i) {
        const SerializedRow& row = dataset[idx];
        b.buf.append(row.prefix);
        b.buf.push_back(',');

        // record where the timestamp will start in the buffer
        size_t pos = b.buf.size();
        b.ts_pos.push_back(pos);

        // write placeholder bytes (to be overridden)
        b.buf.append(TS_WIDTH, '0');

        b.buf.append(row.suffix);
        b.buf.push_back('\n');

        idx = (idx + 1) % dataset.size();
    }
    return b;
}

/// stamps the same timestamp into all placeholder positions
void fillTimestamps(std::string& buf,
                             const std::vector<size_t>& ts_pos,
                             int64_t ts_value)
{
    char ts[32];
    auto [ptr, ec] = std::to_chars(ts, ts + sizeof(ts), ts_value);
    (void)ec; // integer to_chars won't fail with sufficient buffer
    const auto ts_len = static_cast<size_t>(ptr - ts);

    // Left‑pad with '0' if timestamp has fewer digits than TS_WIDTH
    // (keeps field width constant; CSV parsers accept leading zeros)
    for (const auto pos : ts_pos) {
        if (ts_len >= static_cast<size_t>(TS_WIDTH)) {
            // If ever longer (unlikely), copy the rightmost TS_WIDTH digits
            std::memcpy(&buf[pos], ts + (ts_len - TS_WIDTH), TS_WIDTH);
        } else {
            size_t pad = TS_WIDTH - ts_len;
            std::memset(&buf[pos], '0', pad);
            std::memcpy(&buf[pos + pad], ts, ts_len);
        }
    }
}

// disable Nagle to avoid any interaction with batching
void configure_socket(tcp::socket& socket) {
    const asio::ip::tcp::no_delay nd(true);
    socket.set_option(nd);
}

// Minimal usage example (no server loop here):
// Given a connected socket, dataset, and starting index, send m tuples
// with one timestamp (captured immediately before publish).
void send_batch_same_timestamp(tcp::socket& sock,
                               const std::vector<SerializedRow>& dataset,
                               size_t& idx,
                               int m)
{
    // 1) Build batch with placeholders
    Batch b = build_batch_with_placeholders(dataset, idx, m);

    // 2) Capture *one* timestamp immediately before publish
    int64_t t = now_nanos();

    // 3) Overwrite placeholders with the same timestamp
    fillTimestamps(b.buf, b.ts_pos, t);

    // 4) Single syscall to publish
    asio::write(sock, asio::buffer(b.buf));
}



Config load_config(const std::string& path);

// read CSV file into memory (skip header if present)
std::vector<SerializedRow> read_csv(const std::string& filename) {
    std::vector<SerializedRow> dataset;
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << "\n";
        return dataset;
    }

    std::string line;

    // skip header line
    std::getline(file, line);

    std::cout << "Loading the dataset into memory..." << "\n";

    while (std::getline(file, line)) {
        // trim '\r' from Windows-style files
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) continue;
        dataset.push_back({line, ""});
    }
    std::cout << "Loaded " << dataset.size() << " rows into memory";
    if (!dataset.empty()) {
        std::cout << ", first row='" << dataset.front().prefix << "'";
    }
    std::cout << "\n";
    return dataset;
}

namespace {
    uint32_t read_be_u32(const unsigned char* p) {
        return (static_cast<uint32_t>(p[0]) << 24)
               | (static_cast<uint32_t>(p[1]) << 16)
               | (static_cast<uint32_t>(p[2]) << 8)
               | static_cast<uint32_t>(p[3]);
    }

    std::string base64_encode(const std::vector<unsigned char>& bytes) {
        static constexpr char table[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        std::string out;
        out.reserve(((bytes.size() + 2) / 3) * 4);

        size_t i = 0;
        for (; i + 2 < bytes.size(); i += 3) {
            const uint32_t chunk = (static_cast<uint32_t>(bytes[i]) << 16)
                                   | (static_cast<uint32_t>(bytes[i + 1]) << 8)
                                   | static_cast<uint32_t>(bytes[i + 2]);
            out.push_back(table[(chunk >> 18) & 0x3F]);
            out.push_back(table[(chunk >> 12) & 0x3F]);
            out.push_back(table[(chunk >> 6) & 0x3F]);
            out.push_back(table[chunk & 0x3F]);
        }

        if (i < bytes.size()) {
            const uint32_t chunk = (static_cast<uint32_t>(bytes[i]) << 16)
                                   | ((i + 1 < bytes.size()) ? (static_cast<uint32_t>(bytes[i + 1]) << 8) : 0U);
            out.push_back(table[(chunk >> 18) & 0x3F]);
            out.push_back(table[(chunk >> 12) & 0x3F]);
            out.push_back((i + 1 < bytes.size()) ? table[(chunk >> 6) & 0x3F] : '=');
            out.push_back('=');
        }

        return out;
    }

    std::string hex_encode(const std::vector<unsigned char>& bytes) {
        static constexpr char digits[] = "0123456789abcdef";
        std::string out;
        out.reserve(bytes.size() * 2);

        for (unsigned char byte : bytes) {
            out.push_back(digits[byte >> 4]);
            out.push_back(digits[byte & 0x0F]);
        }
        return out;
    }

    std::vector<unsigned char> read_binary_file(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Could not open image file " + path.string());
        }

        file.seekg(0, std::ios::end);
        const auto size = file.tellg();
        file.seekg(0, std::ios::beg);

        if (size < 0) {
            throw std::runtime_error("Could not determine file size for " + path.string());
        }

        const auto size_value = static_cast<size_t>(size);
        std::vector<unsigned char> bytes(size_value);
        if (!bytes.empty()) {
            file.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        }

        if (!file.good() && !file.eof()) {
            throw std::runtime_error("Failed to read image file " + path.string());
        }

        return bytes;
    }

    std::pair<uint32_t, uint32_t> parse_png_dimensions(const std::vector<unsigned char>& bytes,
                                                       const std::filesystem::path& path) {
        static constexpr unsigned char signature[] = {
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A
        };

        if (bytes.size() < 24 || !std::equal(std::begin(signature), std::end(signature), bytes.begin())) {
            throw std::runtime_error("Unsupported or invalid PNG file " + path.string());
        }

        const uint32_t width = read_be_u32(bytes.data() + 16);
        const uint32_t height = read_be_u32(bytes.data() + 20);

        if (width == 0 || height == 0) {
            throw std::runtime_error("Invalid PNG dimensions in " + path.string());
        }

        return {width, height};
    }

    std::vector<SerializedRow> read_image_folder(const std::filesystem::path& folder,
                                                 const std::string& encoding) {
        if (!std::filesystem::exists(folder)) {
            throw std::runtime_error("Image folder does not exist: " + folder.string());
        }
        if (!std::filesystem::is_directory(folder)) {
            throw std::runtime_error("Image source is not a directory: " + folder.string());
        }

        std::vector<std::filesystem::path> files;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(folder)) {
            if (!entry.is_regular_file()) continue;

            std::string ext = entry.path().extension().string();
            std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });

            if (ext == ".png") {
                files.push_back(entry.path());
            }
        }

        std::sort(files.begin(), files.end());

        if (files.empty()) {
            throw std::runtime_error("No PNG files found under " + folder.string());
        }

        const bool use_hex = (encoding == "hex");
        const bool use_base64 = (encoding == "base64");
        if (!use_hex && !use_base64) {
            throw std::runtime_error("Unsupported image encoding '" + encoding + "'. Use 'base64' or 'hex'.");
        }

        std::vector<SerializedRow> dataset;
        dataset.reserve(files.size());

        for (size_t i = 0; i < files.size(); ++i) {
            const auto& path = files[i];
            const auto bytes = read_binary_file(path);
            const auto [width, height] = parse_png_dimensions(bytes, path);

            std::string id = path.stem().string();
            if (id.empty()) {
                id = std::to_string(i);
            }

            const std::string payload = use_hex ? hex_encode(bytes) : base64_encode(bytes);
            dataset.push_back({id, "," + std::to_string(width) + "," + std::to_string(height) + "," + payload});
        }

        std::cout << "Loaded " << dataset.size() << " PNG frames from " << folder
                  << " using " << encoding << " encoding\n";
        return dataset;
    }
}

// Handle TCP clients: send each tuple individually and enqueue for MQTT
void handle_client(tcp::socket socket,
                   const std::vector<SerializedRow>& dataset,
                   int chunk_size,
                   int data_rate,
                   AtomicBool& running,
                   bool mqtt_enabled) {
    try {
        std::cout << "Client connected: " << socket.remote_endpoint() << "\n";
        size_t idx = 0;
        const size_t total = dataset.size();
        const int interval_ms = data_rate;

        while (running.load()) {
            for (int i = 0; i < chunk_size; ++i) {
                // generate timestamp in ms
                auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                std::ostringstream oss;
                oss << dataset[idx].prefix << "," << now_ms/10 << dataset[idx].suffix;
                const std::string msg = oss.str();

                // send tuple over TCP
                asio::write(socket, asio::buffer(msg));

                // enqueue tuple for MQTT
                if (mqtt_enabled) {
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        mqtt_queue.push(msg);
                    }
                    queue_cv.notify_one();
                }

                idx = (idx + 1) % total;

            }

        // optional per-tuple rate control
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }

        socket.close();
        std::cout << "Client disconnected.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in client connection: " << e.what() << "\n";
    }
}

// new
void handle_client_new(tcp::socket socket,
                   const std::vector<SerializedRow>& dataset,
                   int chunk_size,
                   int data_rate,
                   AtomicBool& running,
                   bool mqtt_enabled,
                   int interval_ms) {
    try {
        configure_socket(socket); // disable Nagle; reduce jitter
        std::cout << "Client connected: " << socket.remote_endpoint() << "\n";

        size_t idx = 0;
        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        // Derive batch interval from desired tuple rate.
        // data_rate = tuples/sec, chunk_size = tuples/batch
        // => batches/sec = data_rate / chunk_size
        // => interval_ms = 1000 * chunk_size / data_rate
        //const int tuples_per_sec   = std::max(1, data_rate);
        //const int tuples_per_batch = std::max(1, chunk_size);
        const auto interval = std::chrono::milliseconds(
            std::max(1,interval_ms));

        auto next = std::chrono::steady_clock::now();

        while (running.load()) {
            // 1) Build batch with placeholders; advances idx circularly
            Batch b = build_batch_with_placeholders(dataset, idx, chunk_size);

            // 2) Capture ONE timestamp right before publish
            const int64_t t = now_nanos();

            // 3) Fill the same timestamp into all tuples
            fillTimestamps(b.buf, b.ts_pos, t);

            // 4) Publish in one syscall
            asio::write(socket, asio::buffer(b.buf));

            // 5) Optionally mirror to MQTT (move to avoid copy)
            if (mqtt_enabled) {
                {
                    std::lock_guard<std::mutex> lk(queue_mutex);
                    mqtt_queue.emplace(std::move(b.buf));
                }
                queue_cv.notify_one();
            }

            // 6) Pace sending (sleep-until to minimize drift)
            next += interval;
            std::this_thread::sleep_until(next);
        }

        socket.close();
        std::cout << "Client disconnected.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in client connection: " << e.what() << "\n";
    }
}

void handle_client_poisson(tcp::socket socket,
                   const std::vector<SerializedRow>& dataset,
                   int data_rate_tuples_per_sec,
                   int max_batch_size,                 // cap burst (safety)
                   std::chrono::milliseconds tick,     // e.g., 1ms, 10ms, 100ms, 1s
                   std::atomic<bool>& running)
{
    configure_socket(socket);

    size_t idx = 0;
    const double dt_sec = tick.count() / 1000.0;
    const double lambda_tick = std::max(0.0, data_rate_tuples_per_sec * dt_sec);

    std::poisson_distribution<int> pois(lambda_tick);

    auto next = std::chrono::steady_clock::now();

    while (running.load(std::memory_order_relaxed)) {
        next += tick;

        // Sample how many tuples to send in THIS tick
        int k = pois(rng);
        if (max_batch_size > 0) k = std::min(k, max_batch_size);

        if (k > 0) {
            // Build k tuples with reserved timestamp slots
            Batch b = build_batch_with_placeholders(dataset, idx, k);

            // One timestamp for the whole tick, taken right before publish
            const int64_t ts = now_nanos();
            fillTimestamps(b.buf, b.ts_pos, ts);

            asio::write(socket, asio::buffer(b.buf));
        }

        std::this_thread::sleep_until(next);
    }
}



// Zipfian-over-time pattern: per period, bucket 0 has most tuples and quickly decays.
// This uses a Zipf (power-law) envelope over time buckets, repeating every period.
void handle_client_zipfian(tcp::socket socket,
                           const std::vector<SerializedRow>& dataset,
                           int data_rate_tuples_per_sec,
                           std::chrono::milliseconds tick,
                           int period_ms,
                           double zipf_s,
                           bool stochastic,
                           std::atomic<bool>& running)
{
    try {
        configure_socket(socket);

        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        const int tick_ms = std::max<int>(1, static_cast<int>(tick.count()));
        period_ms = std::max(period_ms, tick_ms);
        const int B = std::max(1, period_ms / tick_ms); // buckets per period
        const double period_sec = static_cast<double>(period_ms) / 1000.0;

        // Expected total tuples per period
        const double expected_total = std::max(0.0, data_rate_tuples_per_sec * period_sec);

        // Precompute normalized Zipf weights over time buckets i=0..B-1
        std::vector<double> w(B);
        double sumw = 0.0;
        for (int i = 0; i < B; ++i) {
            w[i] = 1.0 / std::pow(static_cast<double>(i + 1), zipf_s);
            sumw += w[i];
        }
        if (sumw <= 0.0) sumw = 1.0;
        for (int i = 0; i < B; ++i) w[i] /= sumw;

        // Mean tuples per bucket
        std::vector<double> mu(B);
        for (int i = 0; i < B; ++i) mu[i] = expected_total * w[i];

        // Cap bursts (safety). Largest bucket is bucket 0.
        const int max_k = std::max(1, static_cast<int>(std::ceil(mu[0] * 4.0)));

        std::poisson_distribution<int> pois;
        size_t idx = 0;

        // Align period boundaries to wall-clock (ms) so your phase plots are clean.
        int64_t period_start_ms = (now_nanos() / period_ms) * static_cast<int64_t>(period_ms);

        auto next = std::chrono::steady_clock::now();
        int bucket = 0;

        while (running.load(std::memory_order_relaxed)) {
            next += tick;

            int k = 0;
            if (stochastic) {
                std::poisson_distribution<int>::param_type p(mu[bucket]);
                pois.param(p);
                k = pois(rng);
            } else {
                k = static_cast<int>(std::llround(mu[bucket]));
            }

            if (k > max_k) k = max_k;

            if (k > 0) {
                // Use an exact bucket timestamp (quantized), not "now" (avoids jitter).
                const int64_t ts = period_start_ms + static_cast<int64_t>(bucket) * tick_ms;

                Batch b = build_batch_with_placeholders(dataset, idx, k);
                fillTimestamps(b.buf, b.ts_pos, ts);

                asio::write(socket, asio::buffer(b.buf));
            }

            bucket++;
            if (bucket >= B) {
                bucket = 0;
                period_start_ms += static_cast<int64_t>(period_ms);
            }

            std::this_thread::sleep_until(next);
        }

        socket.close();
    } catch (const std::exception& e) {
        std::cerr << "Error in zipfian client connection: " << e.what() << "\n";
    }
}


// -----------------------------------------------------------------------------
// Period-based patterns:
// - data_rate means "tuples per period" (NOT tuples per second).
// - period_ms defines the length of that period in milliseconds.
// - We derive the number of "buckets" per period from chunk_size so we can pace
//   output without an explicit "interval" parameter.
//   buckets = ceil(data_rate / chunk_size) (at least 1)
//   tick = period / buckets
// -----------------------------------------------------------------------------

static inline int clamp_int(int v, int lo, int hi) {
    return std::max(lo, std::min(hi, v));
}
static inline double clamp_double(double v, double lo, double hi) {
    return std::max(lo, std::min(hi, v));
}

// Evenly split 'total' items into 'buckets' buckets.
// Returns vector k where sum(k)=total and k differs by at most 1 between buckets.
static std::vector<int> even_split(int total, int buckets) {
    buckets = std::max(1, buckets);
    total = std::max(0, total);
    std::vector<int> out(buckets, 0);
    const int base = total / buckets;
    const int rem  = total % buckets;
    for (int i = 0; i < buckets; ++i) out[i] = base + (i < rem ? 1 : 0);
    return out;
}

// Uniform: spread data_rate tuples evenly over the whole period.
void handle_client_uniform_period(tcp::socket socket,
                                  const std::vector<SerializedRow>& dataset,
                                  int data_rate_per_period,
                                  int chunk_size,
                                  int period_ms,
                                  int buckets,
                                  AtomicBool& running,
                                  bool mqtt_enabled)
{
    try {
        configure_socket(socket);
        std::cout << "Client connected: " << socket.remote_endpoint() << "\n";

        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        period_ms = std::max(1, period_ms);
        buckets   = std::max(1, buckets);

        const auto tick = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::milliseconds(period_ms)) / buckets;

        // Deterministic schedule per period (repeatable)
        const std::vector<int> per_bucket = even_split(data_rate_per_period, buckets);

        size_t idx = 0;

        // Align to period boundary in wall-clock milliseconds
        int64_t period_start_ms = (now_nanos() / period_ms) * static_cast<int64_t>(period_ms);

        auto next = std::chrono::steady_clock::now();
        int bucket = 0;

        while (running.load(std::memory_order_relaxed)) {
            next += tick;

            const int k = per_bucket[bucket];
            if (k > 0) {
                // Quantized, stable timestamp (good for phase plots)
                const int64_t ts = period_start_ms + (static_cast<int64_t>(bucket) * period_ms) / buckets;

                Batch b = build_batch_with_placeholders(dataset, idx, k);
                fillTimestamps(b.buf, b.ts_pos, ts);

                asio::write(socket, asio::buffer(b.buf));

                if (mqtt_enabled) {
                    std::lock_guard<std::mutex> lk(queue_mutex);
                    mqtt_queue.emplace(std::move(b.buf));
                    queue_cv.notify_one();
                }
            }

            bucket++;
            if (bucket >= buckets) {
                bucket = 0;
                period_start_ms += static_cast<int64_t>(period_ms);
            }

            std::this_thread::sleep_until(next);
        }

        socket.close();
        std::cout << "Client disconnected.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error in uniform client connection: " << e.what() << "\n";
    }
}

// Poisson: each bucket draws k ~ Poisson(lambda), where lambda = data_rate / buckets.
// Expected total per period = data_rate.
void handle_client_poisson_period(tcp::socket socket,
                                  const std::vector<SerializedRow>& dataset,
                                  int data_rate_per_period,
                                  int period_ms,
                                  int buckets,
                                  AtomicBool& running)
{
    try {
        configure_socket(socket);

        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        period_ms = std::max(1, period_ms);
        buckets   = std::max(1, buckets);

        const double lambda = std::max(0.0, static_cast<double>(data_rate_per_period) / buckets);
        std::poisson_distribution<int> pois(lambda);

        const int max_batch = std::max(1, static_cast<int>(std::ceil(lambda * 4.0)));

        const auto tick = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::milliseconds(period_ms)) / buckets;

        size_t idx = 0;
        int64_t period_start_ms = (now_nanos() / period_ms) * static_cast<int64_t>(period_ms);

        auto next = std::chrono::steady_clock::now();
        int bucket = 0;

        while (running.load(std::memory_order_relaxed)) {
            next += tick;

            int k = pois(rng);
            if (k > max_batch) k = max_batch;

            if (k > 0) {
                const int64_t ts = period_start_ms + (static_cast<int64_t>(bucket) * period_ms) / buckets;

                Batch b = build_batch_with_placeholders(dataset, idx, k);
                fillTimestamps(b.buf, b.ts_pos, ts);
                asio::write(socket, asio::buffer(b.buf));
            }

            bucket++;
            if (bucket >= buckets) {
                bucket = 0;
                period_start_ms += static_cast<int64_t>(period_ms);
            }

            std::this_thread::sleep_until(next);
        }

        socket.close();
    } catch (const std::exception& e) {
        std::cerr << "Error in poisson client connection: " << e.what() << "\n";
    }
}

// Zipfian envelope over time buckets, repeating every period_ms.
// Total expected tuples per period = data_rate_per_period.
void handle_client_zipfian_period(tcp::socket socket,
                                  const std::vector<SerializedRow>& dataset,
                                  int data_rate_per_period,
                                  int period_ms,
                                  int buckets,
                                  double zipf_s,
                                  bool stochastic,
                                  AtomicBool& running)
{
    try {
        configure_socket(socket);

        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        period_ms = std::max(1, period_ms);
        buckets   = std::max(1, buckets);

        // Precompute normalized Zipf weights over buckets i=0..B-1
        std::vector<double> w(buckets);
        double sumw = 0.0;
        for (int i = 0; i < buckets; ++i) {
            w[i] = 1.0 / std::pow(static_cast<double>(i + 1), zipf_s);
            sumw += w[i];
        }
        if (sumw <= 0.0) sumw = 1.0;
        for (int i = 0; i < buckets; ++i) w[i] /= sumw;

        // Mean tuples per bucket (expected total per period = data_rate_per_period)
        std::vector<double> mu(buckets);
        for (int i = 0; i < buckets; ++i) mu[i] = std::max(0.0, static_cast<double>(data_rate_per_period) * w[i]);

        // Cap bursts (safety). Largest bucket is bucket 0.
        const int max_k = std::max(1, static_cast<int>(std::ceil(mu[0] * 4.0)));

        std::poisson_distribution<int> pois;
        size_t idx = 0;

        int64_t period_start_ms = (now_nanos() / period_ms) * static_cast<int64_t>(period_ms);

        const auto tick = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::milliseconds(period_ms)) / buckets;

        auto next = std::chrono::steady_clock::now();
        int bucket = 0;

        while (running.load(std::memory_order_relaxed)) {
            next += tick;

            int k = 0;
            if (stochastic) {
                std::poisson_distribution<int>::param_type p(mu[bucket]);
                pois.param(p);
                k = pois(rng);
            } else {
                k = static_cast<int>(std::llround(mu[bucket]));
            }
            if (k > max_k) k = max_k;

            if (k > 0) {
                const int64_t ts = period_start_ms + (static_cast<int64_t>(bucket) * period_ms) / buckets;

                Batch b = build_batch_with_placeholders(dataset, idx, k);
                fillTimestamps(b.buf, b.ts_pos, ts);
                asio::write(socket, asio::buffer(b.buf));
            }

            bucket++;
            if (bucket >= buckets) {
                bucket = 0;
                period_start_ms += static_cast<int64_t>(period_ms);
            }

            std::this_thread::sleep_until(next);
        }

        socket.close();
    } catch (const std::exception& e) {
        std::cerr << "Error in zipfian client connection: " << e.what() << "\n";
    }
}

// Burst: fraction of tuples happen in the first distribution fraction of the period.
// Example: fraction=0.9, distribution=0.2 -> 90% tuples in first 20% of period.
void handle_client_burst_period(tcp::socket socket,
                                const std::vector<SerializedRow>& dataset,
                                int data_rate_per_period,
                                int period_ms,
                                int buckets,
                                double fraction,
                                double distribution,
                                AtomicBool& running)
{
    try {
        configure_socket(socket);

        if (dataset.empty()) {
            std::cerr << "Dataset is empty; closing connection.\n";
            socket.close();
            return;
        }

        period_ms = std::max(1, period_ms);
        buckets   = std::max(1, buckets);

        fraction     = clamp_double(fraction, 0.0, 1.0);
        distribution = clamp_double(distribution, 0.0, 1.0);

        const int total = std::max(0, data_rate_per_period);

        const int burst_buckets = std::max(1, static_cast<int>(std::ceil(distribution * buckets)));
        const int burst_total   = clamp_int(static_cast<int>(std::llround(fraction * total)), 0, total);
        const int rest_total    = total - burst_total;

        std::vector<int> per_bucket(buckets, 0);
        auto first = even_split(burst_total, burst_buckets);
        for (int i = 0; i < burst_buckets; ++i) per_bucket[i] = first[i];

        if (burst_buckets < buckets) {
            auto rest = even_split(rest_total, buckets - burst_buckets);
            for (int i = 0; i < (buckets - burst_buckets); ++i) per_bucket[burst_buckets + i] = rest[i];
        }

        const auto tick = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::milliseconds(period_ms)) / buckets;

        size_t idx = 0;
        int64_t period_start_ms = (now_nanos() / period_ms) * static_cast<int64_t>(period_ms);

        auto next = std::chrono::steady_clock::now();
        int bucket = 0;

        while (running.load(std::memory_order_relaxed)) {
            next += tick;

            const int k = per_bucket[bucket];
            if (k > 0) {
                const int64_t ts = period_start_ms + (static_cast<int64_t>(bucket) * period_ms) / buckets;

                Batch b = build_batch_with_placeholders(dataset, idx, k);
                fillTimestamps(b.buf, b.ts_pos, ts);
                asio::write(socket, asio::buffer(b.buf));
            }

            bucket++;
            if (bucket >= buckets) {
                bucket = 0;
                period_start_ms += static_cast<int64_t>(period_ms);
            }

            std::this_thread::sleep_until(next);
        }

        socket.close();
    } catch (const std::exception& e) {
        std::cerr << "Error in burst client connection: " << e.what() << "\n";
    }
}


// TCP server thread: accept connections and spawn handlers

void startTcpServer(const std::string& host,
                    int port,
                    const std::vector<SerializedRow>& dataset,
                    int chunk_size,
                    int data_rate_per_period,
                    int period_ms,
                    const std::string& pattern,
                    double pattern_fraction,
                    double pattern_distribution,
                    AtomicBool& running,
                    bool mqtt_enabled,
                    // zipfian options (optional)
                    int zipf_period_ms,
                    double zipf_s,
                    bool zipf_stochastic)
{
    try {
        auto io_context = std::make_shared<asio::io_context>();
        tcp::acceptor acceptor(*io_context, tcp::endpoint(asio::ip::make_address(host), port));

        period_ms = std::max(1, period_ms);
        const int cs = std::max(1, chunk_size);
        const int tuples = std::max(0, data_rate_per_period);

        // Derive number of buckets per period from chunk_size.
        // This controls pacing resolution without an explicit "interval" parameter.
        const int buckets = std::max(1, (tuples + cs - 1) / cs);

        std::cout << "TCP connection ready at " << host << ":" << port
                  << " (pattern=" << pattern
                  << ", data_rate=" << tuples << " tuples/period"
                  << ", period=" << period_ms << "ms"
                  << ", buckets=" << buckets << ")\n";

        while (running.load(std::memory_order_relaxed)) {
            tcp::socket sock(*io_context);
            acceptor.accept(sock);

            if (pattern == "uniform") {
                std::thread([socket = std::move(sock),
                             &dataset,
                             tuples,
                             cs,
                             period_ms,
                             buckets,
                             &running,
                             mqtt_enabled,
                             io_context]() mutable {
                    handle_client_uniform_period(std::move(socket),
                                                 dataset,
                                                 tuples,
                                                 cs,
                                                 period_ms,
                                                 buckets,
                                                 running,
                                                 mqtt_enabled);
                }).detach();

            } else if (pattern == "poisson") {
                std::thread([socket = std::move(sock),
                             &dataset,
                             tuples,
                             period_ms,
                             buckets,
                             &running,
                             io_context]() mutable {
                    handle_client_poisson_period(std::move(socket),
                                                 dataset,
                                                 tuples,
                                                 period_ms,
                                                 buckets,
                                                 running);
                }).detach();

            } else if (pattern == "zipfian") {
                // Enforce that the pattern period equals the global period.
                if (zipf_period_ms != period_ms) {
                    std::cerr << "Warning: zipf.period_ms (" << zipf_period_ms
                              << ") differs from period (" << period_ms
                              << "). Using period for zipfian scheduling.\n";
                }
                std::thread([socket = std::move(sock),
                             &dataset,
                             tuples,
                             period_ms,
                             buckets,
                             zipf_s,
                             zipf_stochastic,
                             &running,
                             io_context]() mutable {
                    handle_client_zipfian_period(std::move(socket),
                                                 dataset,
                                                 tuples,
                                                 period_ms,
                                                 buckets,
                                                 zipf_s,
                                                 zipf_stochastic,
                                                 running);
                }).detach();

            } else if (pattern == "burst") {
                std::thread([socket = std::move(sock),
                             &dataset,
                             tuples,
                             period_ms,
                             buckets,
                             pattern_fraction,
                             pattern_distribution,
                             &running,
                             io_context]() mutable {
                    handle_client_burst_period(std::move(socket),
                                               dataset,
                                               tuples,
                                               period_ms,
                                               buckets,
                                               pattern_fraction,
                                               pattern_distribution,
                                               running);
                }).detach();

            } else {
                std::cerr << "Unknown pattern: " << pattern << " (falling back to uniform)\n";
                std::thread([socket = std::move(sock),
                             &dataset,
                             tuples,
                             cs,
                             period_ms,
                             buckets,
                             &running,
                             mqtt_enabled,
                             io_context]() mutable {
                    handle_client_uniform_period(std::move(socket),
                                                 dataset,
                                                 tuples,
                                                 cs,
                                                 period_ms,
                                                 buckets,
                                                 running,
                                                 mqtt_enabled);
                }).detach();
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in TCP server: " << e.what() << "\n";
    }
}


// MQTT publisher thread: dequeue and publish messages one by one
void mqtt_publisher(mosquitto* client,
                    AtomicBool& running,
                    const std::string& topic) {
    while (running.load() || !mqtt_queue.empty()) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait(lock, []{
            return !mqtt_queue.empty() || std::uncaught_exceptions() > 0;
        });
        while (!mqtt_queue.empty()) {
            std::string msg = std::move(mqtt_queue.front());
            mqtt_queue.pop();
            lock.unlock();

            int ret = mosquitto_publish(client,
                                        nullptr,
                                        topic.c_str(),
                                        static_cast<int>(msg.size()),
                                        msg.c_str(),
                                        1,
                                        false);
            if (ret != MOSQ_ERR_SUCCESS) {
                std::cerr << "MQTT publish error: " << mosquitto_strerror(ret) << "\n";
            }

            lock.lock();
        }
    }
}

// Read YAML configuration

// Read YAML configuration
bool read_config(const std::string& cfg,
                 std::string& host,
                 int& period_ms,
                 std::string& pattern_type,
                 double& pattern_fraction,
                 double& pattern_distribution,
                 std::string& data_source_type,
                 std::string& data_file,
                 std::string& image_encoding,
                 int& duration,
                 int& data_rate_per_period,
                 int& chunk_size,
                 std::vector<int>& ports,
                 bool& mqtt_enabled,
                 std::string& mqtt_host,
                 int& mqtt_port,
                 std::string& mqtt_topic,
                 // zipfian options (optional; used only when pattern_type=zipfian)
                 int& zipf_period_ms,
                 double& zipf_s,
                 bool& zipf_stochastic)
{
    try {
        YAML::Node config = YAML::LoadFile(cfg);

        // host
        host = "0.0.0.0";
        if (config["host"].IsDefined()) host = config["host"].as<std::string>();

        // period (ms): NEW. We keep 'interval' only as a legacy alias.
        period_ms = 1000;
        if (config["period"].IsDefined()) {
            period_ms = config["period"].as<int>();
        } else if (config["interval"].IsDefined()) {
            period_ms = config["interval"].as<int>();
            std::cerr << "Warning: 'interval' is deprecated; rename it to 'period' (ms). "
                      << "Using interval as period for now.\n";
        }
        period_ms = std::max(1, period_ms);

        // core throughput controls
        duration = config["duration"].IsDefined() ? config["duration"].as<int>() : 10;
        data_rate_per_period = config["data_rate"].IsDefined() ? config["data_rate"].as<int>() : 1000;
        chunk_size = config["chunk_size"].IsDefined() ? config["chunk_size"].as<int>() : 10;

        data_rate_per_period = std::max(0, data_rate_per_period);
        chunk_size = std::max(1, chunk_size);

        // dataset
        data_source_type = "csv";
        image_encoding = "base64";
        if (config["data_source"].IsDefined()) {
            if (config["data_source"]["type"].IsDefined()) {
                data_source_type = config["data_source"]["type"].as<std::string>();
            }
            if (config["data_source"]["file"].IsDefined()) {
                data_file = config["data_source"]["file"].as<std::string>();
            }
            if (config["data_source"]["encoding"].IsDefined()) {
                image_encoding = config["data_source"]["encoding"].as<std::string>();
            }
        }

        // pattern (default uniform)
        pattern_type = "uniform";
        pattern_fraction = 0.9;      // defaults (relevant for burst only)
        pattern_distribution = 0.2;  // defaults (relevant for burst only)

        if (config["pattern"].IsDefined()) {
            YAML::Node p = config["pattern"];

            auto apply_kv = [&](const std::string& k, const YAML::Node& v) {
                if (k == "type") {
                    pattern_type = v.as<std::string>();
                } else if (k == "fraction") {
                    pattern_fraction = v.as<double>();
                } else if (k == "distribution") {
                    pattern_distribution = v.as<double>();
                }
            };

            if (p.IsScalar()) {
                pattern_type = p.as<std::string>();
            } else if (p.IsMap()) {
                for (auto it = p.begin(); it != p.end(); ++it) {
                    apply_kv(it->first.as<std::string>(), it->second);
                }
            } else if (p.IsSequence()) {
                // Your preferred style: list of small maps, e.g.
                // pattern:
                //   - type: burst
                //   - fraction: 0.9
                //   - distribution: 0.8
                for (const auto& item : p) {
                    if (!item.IsMap()) continue;
                    for (auto it = item.begin(); it != item.end(); ++it) {
                        apply_kv(it->first.as<std::string>(), it->second);
                    }
                }
            }
        }

        // ports
        ports.clear();
        for (auto p : config["ports"]) {
            ports.push_back(p.as<int>());
        }

        // MQTT section (optional)
        mqtt_enabled = false;
        if (config["mqtt"].IsDefined() && config["mqtt"]["enabled"].IsDefined()) {
            mqtt_enabled = config["mqtt"]["enabled"].as<bool>();
            if (mqtt_enabled) {
                mqtt_host  = config["mqtt"]["host"].as<std::string>();
                mqtt_port  = config["mqtt"]["port"].as<int>();
                mqtt_topic = config["mqtt"]["topic"].as<std::string>();
            }
        }

        // Zipf options (optional)
        zipf_period_ms = period_ms; // keep aligned with global period by default
        zipf_s = 1.4;
        zipf_stochastic = true;
        if (config["zipf"].IsDefined()) {
            auto z = config["zipf"];
            if (z["period_ms"].IsDefined())   zipf_period_ms = z["period_ms"].as<int>();
            if (z["s"].IsDefined())           zipf_s         = z["s"].as<double>();
            if (z["stochastic"].IsDefined())  zipf_stochastic = z["stochastic"].as<bool>();
        }

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Config read error: " << e.what() << "\n";
        return false;
    }
}


int main(int argc, char *argv[]) {
    auto configPath = std::string("config.yaml");
    if (argc > 1) {
        configPath = argv[1];
    }

    std::string host, pattern, data_file, mqtt_host, mqtt_topic;
    std::string data_source_type, image_encoding;
    int period_ms = 1000, duration_s = 0, data_rate = 0, chunk_size = 0, mqtt_port = 0;
    double pattern_fraction = 0.9, pattern_distribution = 0.2;
    std::vector<int> ports;
    bool mqtt_enabled = false;

    int zipf_period_ms = 1000;
    double zipf_s = 1.4;
    bool zipf_stochastic = true;

    if (!read_config(configPath,
                     host, period_ms, pattern, pattern_fraction, pattern_distribution,
                     data_source_type, data_file, image_encoding,
                     duration_s, data_rate, chunk_size,
                     ports,
                     mqtt_enabled, mqtt_host, mqtt_port, mqtt_topic,
                     zipf_period_ms, zipf_s, zipf_stochastic)) {
        return EXIT_FAILURE;
    }

    std::cout << "Config: pattern=" << pattern
              << " data_source=" << data_source_type
              << " data_rate=" << data_rate << " tuples/period"
              << " period=" << period_ms << "ms"
              << " duration=" << duration_s << "s"
              << " ports=" << ports.size()
              << " fraction=" << pattern_fraction
              << " distribution=" << pattern_distribution
              << "\n";

    // Load dataset into memory once
    std::vector<SerializedRow> dataset;
    try {
        if (data_source_type == "csv") {
            dataset = read_csv(data_file);
        } else if (data_source_type == "image_folder") {
            dataset = read_image_folder(data_file, image_encoding);
        } else {
            std::cerr << "Unsupported data_source.type: " << data_source_type << "\n";
            return EXIT_FAILURE;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to load dataset: " << e.what() << "\n";
        return EXIT_FAILURE;
    }
    if (dataset.empty()) {
        std::cerr << "No data loaded from " << data_file << "\n";
        return EXIT_FAILURE;
    }

    mosquitto* mqtt_client = nullptr;
    std::thread mqtt_thread;
    AtomicBool running(true);

    if (mqtt_enabled) {
        mosquitto_lib_init();
        mqtt_client = mosquitto_new(nullptr, true, nullptr);
        if (!mqtt_client ||
            mosquitto_connect(mqtt_client, mqtt_host.c_str(), mqtt_port, 60) != MOSQ_ERR_SUCCESS) {
            std::cerr << "MQTT init/connect error\n";
            return EXIT_FAILURE;
        }
        mosquitto_loop_start(mqtt_client);
        mqtt_thread = std::thread(mqtt_publisher,
                                  mqtt_client,
                                  std::ref(running),
                                  mqtt_topic);
    }

    std::vector<std::thread> tcp_threads;
    tcp_threads.reserve(ports.size());
    for (int port : ports) {
        tcp_threads.emplace_back(startTcpServer,
                                 host,
                                 port,
                                 std::cref(dataset),
                                 chunk_size,
                                 data_rate,
                                 period_ms,
                                 pattern,
                                 pattern_fraction,
                                 pattern_distribution,
                                 std::ref(running),
                                 mqtt_enabled,
                                 zipf_period_ms,
                                 zipf_s,
                                 zipf_stochastic);
    }

    std::this_thread::sleep_for(std::chrono::seconds(duration_s));
    running.store(false);
    queue_cv.notify_all();

    for (auto& t : tcp_threads) {
        if (t.joinable()) t.join();
    }

    if (mqtt_enabled) {
        if (mqtt_thread.joinable()) mqtt_thread.join();
        mosquitto_loop_stop(mqtt_client, true);
        mosquitto_disconnect(mqtt_client);
        mosquitto_destroy(mqtt_client);
        mosquitto_lib_cleanup();
    }

    std::cout << "exiting datagen...\n";
    return EXIT_SUCCESS;
}
