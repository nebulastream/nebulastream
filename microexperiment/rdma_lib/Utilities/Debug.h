//
// Created by Toso, Lorenzo on 2018-12-12.
//

#ifndef MAMPI_DEBUG_H
#define MAMPI_DEBUG_H


//#define DEBUG
//#define SOME_DEBUG
//#define DEBUG
#ifdef DEBUG
#define SOME_DEBUG
#define TRACE(...) {{printf(__VA_ARGS__); fflush(stdout);}}
#else
#define TRACE(...) {}
#endif


#ifdef SOME_DEBUG
#define TRACE2(...) {{printf(__VA_ARGS__); fflush(stdout);}}
#else
#define TRACE2(...) {}
#endif

#define QUICK_PRINT(...) {{printf(__VA_ARGS__); fflush(stdout);}}


#endif //MAMPI_DEBUG_H
