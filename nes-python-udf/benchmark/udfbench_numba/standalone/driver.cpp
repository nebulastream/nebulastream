/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Runs nes_numba_entry (a Numba-compiled UDF with a C ABI) over inputs.bin and writes outputs.bin; prints ns per row.
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

extern "C" {
// Numba's calling convention: status, then a pointer for the result and one for the exception info, then the arguments.
int32_t nes_numba_entry(int64_t* result, void** exception, const uint8_t* data, int64_t length, uint8_t* out, int64_t capacity);
void NRT_MemSys_init(void);
}

int main(int argc, char** argv)
{
    const std::string dir = argv[1];
    const int rounds = argc > 2 ? std::stoi(argv[2]) : 3;
    NRT_MemSys_init();
    std::ifstream in(dir + "/inputs.bin", std::ios::binary);
    int64_t count = 0;
    in.read(reinterpret_cast<char*>(&count), 8);
    std::vector<std::string> rows(count);
    for (auto& row : rows)
    {
        int64_t length = 0;
        in.read(reinterpret_cast<char*>(&length), 8);
        row.resize(length);
        in.read(row.data(), length);
    }
    std::vector<uint8_t> out(1 << 20);
    std::ofstream results(dir + "/outputs.bin", std::ios::binary);
    results.write(reinterpret_cast<const char*>(&count), 8);
    double best = 1e30;
    for (int round = 0; round < rounds; ++round)
    {
        const auto start = std::chrono::steady_clock::now();
        for (const auto& row : rows)
        {
            int64_t value = 0;
            void* exception = nullptr;
            const int32_t status = nes_numba_entry(&value, &exception, reinterpret_cast<const uint8_t*>(row.data()), static_cast<int64_t>(row.size()),
                                                   out.data(), static_cast<int64_t>(out.size()));
            if (round == 0)
            {
                const int64_t code = status != 0 ? -3 : value;
                results.write(reinterpret_cast<const char*>(&code), 8);
                if (code >= 0)
                    results.write(reinterpret_cast<const char*>(out.data()), code);
            }
        }
        best = std::min(best, std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }
    std::printf("%s rows %lld ns/row %.1f\n", dir.c_str(), static_cast<long long>(count), best / count * 1e9);
}
