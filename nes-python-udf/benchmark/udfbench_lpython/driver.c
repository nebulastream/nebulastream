// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Calls an LPython UDF (the @ccallable function udf_entry) in a loop over argument rows held in memory, as
// nes-python-udf-runner does for Codon and Numba. Usage: driver ARGS SIGNATURE ROUNDS OUTPUT
//
// ARGS has one line per row: a NULL flag (1 if an argument is NULL), then the arguments, separated by tabs. A NULL row gives
// NULL without a call, as the INLINE entry points do. SIGNATURE is the argument kinds, '-', the return kind (s = str,
// i = i64, f = f64): s-s, s-i, f-f or fs-f. Round 0 writes the results to OUTPUT, the rounds after it are timed. LPython
// allocates each result string and the driver does not free it, so a round has no free cost.
// Prints one JSON line with the ns per row of the first round and the best and median of the timed rounds.

#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern int lpython_main(int argc, char** argv);
extern void udf_entry(void);

typedef char* (*StrStr)(char*);
typedef int64_t (*StrInt)(char*);
typedef double (*FloatFloat)(double);
typedef double (*FloatStrFloat)(double, char*);

typedef struct
{
    int null;
    char* text[2];
    double number[2];
} Row;

static double now(void)
{
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    return (double)time.tv_sec + (double)time.tv_nsec * 1e-9;
}

static int compare(const void* a, const void* b)
{
    const double x = *(const double*)a, y = *(const double*)b;
    return (x > y) - (x < y);
}

int main(int argc, char** argv)
{
    if (argc != 5)
    {
        fprintf(stderr, "usage: driver ARGS SIGNATURE ROUNDS OUTPUT\n");
        return 2;
    }
    const char* signature = argv[2];
    const int rounds = atoi(argv[3]);
    const char* kinds = signature;
    const size_t argumentCount = strchr(signature, '-') - signature;
    const char returns = strchr(signature, '-')[1];

    FILE* input = fopen(argv[1], "r");
    if (!input)
    {
        perror(argv[1]);
        return 2;
    }
    size_t capacity = 1024, rowCount = 0;
    Row* rows = malloc(capacity * sizeof(Row));
    char* line = NULL;
    size_t lineCapacity = 0;
    ssize_t length;
    while ((length = getline(&line, &lineCapacity, input)) >= 0)
    {
        if (length > 0 && line[length - 1] == '\n')
        {
            line[--length] = 0;
        }
        if (rowCount == capacity)
        {
            capacity *= 2;
            rows = realloc(rows, capacity * sizeof(Row));
        }
        Row* row = &rows[rowCount++];
        memset(row, 0, sizeof(Row));
        char* field = strtok(line, "\t");
        row->null = field[0] == '1';
        for (size_t index = 0; index < argumentCount && !row->null; ++index)
        {
            field = strtok(NULL, "\t");
            if (kinds[index] == 's')
            {
                row->text[index] = strdup(field ? field : "");
            }
            else
            {
                row->number[index] = strtod(field, NULL);
            }
        }
    }
    fclose(input);

    char* lpythonArguments[] = {argv[0], NULL};
    lpython_main(1, lpythonArguments); // runs the module's global statements, for example the set of filterstopwords

    FILE* output = fopen(argv[4], "w");
    void* entry = (void*)udf_entry;
    double* seconds = malloc((rounds + 1) * sizeof(double));
    for (int round = 0; round <= rounds; ++round)
    {
        const double started = now();
        for (size_t index = 0; index < rowCount; ++index)
        {
            const Row* row = &rows[index];
            if (row->null)
            {
                if (round == 0)
                {
                    fputs("NULL\n", output);
                }
                continue;
            }
            if (returns == 's')
            {
                char* result = ((StrStr)entry)(row->text[0]);
                if (round == 0)
                {
                    fprintf(output, "%s\n", result);
                }
            }
            else if (returns == 'i')
            {
                const int64_t result = ((StrInt)entry)(row->text[0]);
                if (round == 0)
                {
                    fprintf(output, "%lld\n", (long long)result);
                }
            }
            else
            {
                const double result = argumentCount == 1 ? ((FloatFloat)entry)(row->number[0])
                                                         : ((FloatStrFloat)entry)(row->number[0], row->text[1]);
                if (round == 0)
                {
                    fprintf(output, "%.17g\n", result);
                }
            }
        }
        seconds[round] = now() - started;
    }
    fclose(output);
    qsort(seconds + 1, rounds, sizeof(double), compare);
    const double perRow = 1e9 / (double)rowCount;
    printf("{\"rows\": %zu, \"rounds\": %d, \"first_round_ns_per_row\": %.1f, \"best_ns_per_row\": %.1f, "
           "\"median_ns_per_row\": %.1f}\n",
           rowCount,
           rounds,
           seconds[0] * perRow,
           (rounds > 0 ? seconds[1] : seconds[0]) * perRow,
           (rounds > 0 ? seconds[1 + rounds / 2] : seconds[0]) * perRow);
    return 0;
}
