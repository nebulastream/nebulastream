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

// Calls the @ccallable udf_entry of an LPython case module (codon_diff.py --compiler LPython) on the rows of the harness's
// escaped TSV, from row FIRST on, and appends one escaped result line per row to OUTPUT, flushed per row. LPython has no
// exceptions: a runtime error ends the process, and the harness records an ERROR for that row and starts again at the next.
//
// Usage: driver TYPES RETURNS ROWS FIRST OUTPUT
// TYPES are the argument kinds, one letter each (s = str, i = i64, f = f64, b = bool); RETURNS is one of s, i, f, b.
// On x86-64 System V, integer and pointer arguments take the integer registers in order and doubles the vector registers
// in order, so one function pointer type with 6 integer and 4 double parameters calls every signature.

#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int lpython_main(int argc, char** argv);
extern void udf_entry(void);

typedef char* (*ReturnsStr)(int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, double, double, double, double);
typedef int64_t (*ReturnsInt)(int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, double, double, double, double);
typedef double (*ReturnsFloat)(int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, double, double, double, double);
typedef int32_t (*ReturnsBool)(int64_t, int64_t, int64_t, int64_t, int64_t, int64_t, double, double, double, double);

static char* unescape(const char* field)
{
    size_t length = strlen(field);
    char* out = malloc(length + 1);
    size_t o = 0;
    for (size_t i = 0; i < length; ++i)
    {
        if (field[i] == '\\' && i + 1 < length)
        {
            char next = field[++i];
            if (next == 't')
                out[o++] = '\t';
            else if (next == 'n')
                out[o++] = '\n';
            else if (next == 'r')
                out[o++] = '\r';
            else if (next == 'x' && i + 2 < length + 1)
            {
                char hex[3] = {field[i + 1], field[i + 2], 0};
                out[o++] = (char)strtol(hex, NULL, 16);
                i += 2;
            }
            else
                out[o++] = next;
        }
        else
            out[o++] = field[i];
    }
    out[o] = 0;
    return out;
}

static void escape(FILE* out, const char* text)
{
    for (const unsigned char* p = (const unsigned char*)text; *p; ++p)
    {
        if (*p == '\\')
            fputs("\\\\", out);
        else if (*p < 0x20 || *p == 0x7f)
            fprintf(out, "\\x%02x", *p);
        else
            fputc(*p, out);
    }
}

int main(int argc, char** argv)
{
    if (argc != 6)
    {
        fprintf(stderr, "usage: driver TYPES RETURNS ROWS FIRST OUTPUT\n");
        return 64;
    }
    const char* types = argv[1];
    const char returns = argv[2][0];
    FILE* rows = fopen(argv[3], "r");
    const long first = atol(argv[4]);
    FILE* out = fopen(argv[5], "a");
    if (!rows || !out)
    {
        perror("open");
        return 66;
    }
    char* line = NULL;
    size_t capacity = 0;
    long row = 0;
    while (getline(&line, &capacity, rows) >= 0)
    {
        if (row++ < first)
            continue;
        line[strcspn(line, "\n")] = 0;
        int64_t integers[6] = {0};
        double doubles[4] = {0};
        int ni = 0;
        int nd = 0;
        char* rest = line;
        for (const char* t = types; *t; ++t)
        {
            // strsep keeps empty fields: an empty string argument is an empty field.
            const char* field = strsep(&rest, "\t");
            const char* value = field ? field : "";
            if (*t == 's')
                integers[ni++] = (int64_t)(intptr_t)unescape(value);
            else if (*t == 'i')
                integers[ni++] = strtoll(value, NULL, 10);
            else if (*t == 'b')
                integers[ni++] = strcmp(value, "1") == 0;
            else
                doubles[nd++] = strtod(value, NULL);
        }
        void* entry = (void*)&udf_entry;
#define ARGS integers[0], integers[1], integers[2], integers[3], integers[4], integers[5], doubles[0], doubles[1], doubles[2], doubles[3]
        if (returns == 's')
            escape(out, ((ReturnsStr)entry)(ARGS));
        else if (returns == 'i')
            fprintf(out, "%lld", (long long)((ReturnsInt)entry)(ARGS));
        else if (returns == 'b')
            fprintf(out, "%d", ((ReturnsBool)entry)(ARGS) != 0);
        else
            fprintf(out, "%.17g", ((ReturnsFloat)entry)(ARGS));
        fputc('\n', out);
        fflush(out);
    }
    return 0;
}
