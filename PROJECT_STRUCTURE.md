# NebulaStream Project Structure

This document provides a comprehensive overview of the NebulaStream project structure, describing each component and how they are interconnected to form the complete IoT data management system.

## Overview

NebulaStream is a general-purpose, end-to-end data management system for IoT applications. The project follows a modular architecture with clearly separated concerns, organized into distinct components that work together to provide stream processing capabilities.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        NebulaStream System                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────────┐ │
│  │ nes-sources │  │ nes-sinks    │  │ nes-input-formatters    │ │
│  └─────────────┘  └──────────────┘  └─────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────┐  ┌──────────────────────────┐   │
│  │ nes-logical-operators       │  │ nes-physical-operators   │   │
│  └─────────────────────────────┘  └──────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ nes-query-      │  │ nes-query-  │  │ nes-query-engine    │  │
│  │ optimizer       │  │ compiler    │  │                     │  │
│  └─────────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ nes-nautilus    │  │ nes-runtime │  │ nes-memory          │  │
│  └─────────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ nes-data-types  │  │ nes-common  │  │ nes-configurations  │  │
│  └─────────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Core Directories

### Core Engine Components

#### `nes-query-engine/`
**Purpose**: Central query execution engine
- **Description**: The main query execution engine that orchestrates query processing
- **Key Files**: `QueryEngine.cpp`, `RunningQueryPlan.cpp`, `Task.hpp`
- **Dependencies**: All other `nes-*` modules
- **Role**: Coordinates query execution across the entire system

#### `nes-runtime/`
**Purpose**: Runtime execution environment
- **Description**: Provides the runtime environment for executing compiled queries
- **Dependencies**: `nes-common`, `nes-memory`, `nes-data-types`
- **Role**: Manages the execution context and runtime state

#### `nes-memory/`
**Purpose**: Memory management subsystem
- **Description**: Handles memory allocation, buffer management, and garbage collection
- **Dependencies**: `nes-common`
- **Role**: Provides efficient memory management for high-throughput streaming

### Query Processing Pipeline

#### `nes-logical-operators/`
**Purpose**: Logical query operator definitions
- **Description**: Defines logical operators (select, project, join, window, etc.) used in query plans
- **Dependencies**: `nes-common`, `nes-data-types`
- **Connects to**: `nes-query-optimizer` (for logical planning)

#### `nes-physical-operators/`
**Purpose**: Physical operator implementations
- **Description**: Contains concrete implementations of operators for execution
- **Dependencies**: `nes-logical-operators`, `nes-data-types`, `nes-memory`
- **Connects to**: `nes-query-compiler` (for code generation)

#### `nes-query-optimizer/`
**Purpose**: Query optimization and planning
- **Description**: Transforms logical query plans into optimized physical plans
- **Dependencies**: `nes-logical-operators`, `nes-physical-operators`
- **Role**: Cost-based optimization and plan selection

#### `nes-query-compiler/`
**Purpose**: Query compilation to native code
- **Description**: Compiles query plans to efficient native code
- **Dependencies**: `nes-physical-operators`, `nes-nautilus`
- **Role**: Code generation and JIT compilation

### Compilation Framework

#### `nes-nautilus/`
**Purpose**: JIT compilation and code generation framework
- **Description**: Provides Just-In-Time compilation capabilities for high-performance query execution
- **Dependencies**: `nes-common`, LLVM/MLIR
- **Role**: Translates query plans to optimized machine code

### Data Management

#### `nes-data-types/`
**Purpose**: Type system and schema management
- **Description**: Defines data types, schemas, and type operations
- **Dependencies**: `nes-common`
- **Role**: Provides type safety and schema evolution

#### `nes-sources/`
**Purpose**: Data source connectors
- **Description**: Interfaces for various data sources (files, databases, sensors, etc.)
- **Dependencies**: `nes-data-types`, `nes-common`
- **Examples**: File sources, network sources, sensor data sources

#### `nes-sinks/`
**Purpose**: Data sink connectors
- **Description**: Output destinations for processed data
- **Dependencies**: `nes-data-types`, `nes-common`
- **Examples**: File sinks, database sinks, network sinks

#### `nes-input-formatters/`
**Purpose**: Data format parsers and serializers
- **Description**: Handles various input/output formats (CSV, JSON, Protobuf, etc.)
- **Dependencies**: `nes-data-types`
- **Role**: Data parsing and serialization

### Foundation

#### `nes-common/`
**Purpose**: Shared utilities and foundational components
- **Description**: Common utilities, logging, configuration, and base classes
- **Dependencies**: None (foundation layer)
- **Role**: Provides shared infrastructure for all components

#### `nes-configurations/`
**Purpose**: Configuration management
- **Description**: System configuration, parameter management, and settings
- **Dependencies**: `nes-common`
- **Role**: Centralized configuration management

### Applications and Tools

#### `nes-executable/`
**Purpose**: Main executable applications
- **Description**: Contains the main NebulaStream executable and CLI tools
- **Dependencies**: All `nes-*` modules
- **Role**: Entry point for the system

#### `nes-single-node-worker/`
**Purpose**: Single-node execution environment
- **Description**: Standalone worker for single-node deployments
- **Dependencies**: `nes-query-engine`, `nes-runtime`
- **Role**: Simplified deployment option

#### `nes-nebuli/`
**Purpose**: Distributed system coordination
- **Description**: Handles distributed query execution and coordination
- **Dependencies**: `nes-query-engine`, gRPC
- **Role**: Multi-node orchestration

#### `nes-plugins/`
**Purpose**: Plugin system and extensions
- **Description**: Dynamic plugin loading and extension framework
- **Dependencies**: Various `nes-*` modules
- **Role**: Extensibility and modularity

### Testing and Quality

#### `nes-systests/`
**Purpose**: System-level integration tests
- **Description**: End-to-end testing and system validation
- **Dependencies**: All `nes-*` modules
- **Role**: Quality assurance and system verification

### Parsing and Language Support

#### `nes-sql-parser/`
**Purpose**: SQL parsing and query language support
- **Description**: Parses SQL queries and converts them to internal representations
- **Dependencies**: `nes-logical-operators`
- **Role**: Query language frontend

## Build and Development Infrastructure

### `cmake/`
**Purpose**: Build system configuration
- **Contents**: CMake modules, dependency management, build scripts
- **Role**: Manages build process and dependencies

### `scripts/`
**Purpose**: Development and maintenance scripts
- **Contents**: 
  - `format.sh` - Code formatting
  - `check_preamble.py` - License header validation
  - `benchmarking/` - Performance testing scripts
- **Role**: Development workflow automation

### `docs/`
**Purpose**: Documentation and guides
- **Structure**:
  - `technical/` - Technical documentation
  - `development/` - Development guides
  - `design/` - Architecture and design documents
- **Role**: Knowledge management and onboarding

### `docker/`
**Purpose**: Containerization and deployment
- **Contents**: Dockerfile and container configuration
- **Role**: Deployment and environment standardization

### `grpc/`
**Purpose**: gRPC service definitions
- **Contents**: Protocol buffer definitions and service contracts
- **Role**: Inter-service communication

## External Dependencies

### `vcpkg/` and `vcpkg-repository/`
**Purpose**: Package management
- **Description**: Microsoft's C++ package manager for dependency management
- **Role**: Manages external library dependencies

## Data Flow and Component Interactions

1. **Data Ingestion**: `nes-sources` → `nes-input-formatters` → `nes-data-types`
2. **Query Processing**: `nes-sql-parser` → `nes-logical-operators` → `nes-query-optimizer`
3. **Code Generation**: `nes-query-optimizer` → `nes-physical-operators` → `nes-query-compiler` → `nes-nautilus`
4. **Execution**: `nes-query-engine` → `nes-runtime` → `nes-memory`
5. **Output**: `nes-sinks` → External systems

## Key Technologies

- **Language**: C++23
- **Build System**: CMake
- **Compilation**: Clang 18+ (required)
- **JIT Compilation**: LLVM/MLIR (via nes-nautilus)
- **Communication**: gRPC and Protocol Buffers
- **Testing**: Google Test framework
- **Package Management**: vcpkg

## Build Types and Configuration

The project supports multiple build configurations:
- **Debug**: Full logging and assertions for development
- **RelWithDebInfo**: Balanced performance with debugging info
- **Release**: Optimized for production with error-level logging
- **Benchmark**: Maximum performance with all checks disabled

This modular architecture enables NebulaStream to provide high-performance stream processing while maintaining flexibility and extensibility for various IoT use cases. 