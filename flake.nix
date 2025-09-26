{
  description = "NebulaStream development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        llvm = pkgs.llvmPackages_19;
        clangStdenv = llvm.stdenv;
        mkShellClang = pkgs.mkShell.override { stdenv = clangStdenv; };

        # MLIR 20 binary release
        mlirBinary = pkgs.stdenvNoCC.mkDerivation rec {
          pname = "nes-mlir";
          version = "20";

          src =
            if pkgs.stdenv.hostPlatform.isAarch64 then
              pkgs.fetchurl {
                url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-arm64-none-libstdcxx.tar.zstd";
                sha256 = "3e74c024e865efac646abe09b2cadcf88530b9af799b2d6fd0a5e5e1e01e685e";
              }
            else if pkgs.stdenv.hostPlatform.isx86_64 then
              pkgs.fetchurl {
                url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-x64-none-libstdcxx.tar.zstd";
                sha256 = "aef98d5bd61a8530392796e86a15a36d7fde9d553579cf64dfd465454e3fae7a";
              }
            else
              throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}";

          nativeBuildInputs = [
            pkgs.zstd
            pkgs.gnutar
          ];

          dontUnpack = true;
          strictDeps = true;

          installPhase = ''
            runHook preInstall
            mkdir -p "$TMPDIR/mlir"
            zstd -d "$src" --stdout | tar -xf - -C "$TMPDIR/mlir"
            mkdir -p "$out"
            cp -r "$TMPDIR/mlir"/clang/* "$out"/
            runHook postInstall
          '';

          meta = with lib; {
            description = "Prebuilt MLIR toolchain used by NebulaStream";
            license = licenses.asl20;
            platforms = platforms.linux;
          };
        };

        antlr4Version = "4.13.2";

        fmtPkg = pkgs.fmt_11;
        spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };

        baseThirdPartyDeps = with pkgs; [
          fmtPkg
          spdlogPkg
          grpc
          protobuf
          abseil-cpp
          nlohmann_json
          yaml-cpp
          folly
          replxx
          magic-enum
          antlr4
          boost
          openssl.dev
          zstd.dev
          zlib.dev
          libdwarf.dev
          libffi
          libxml2
          gflags
          glog
          gtest
          tbb
          python3
          openjdk21
        ];

        antlr4Jar = pkgs.fetchurl {
          url = "https://www.antlr.org/download/antlr-${antlr4Version}-complete.jar";
          hash = "sha256-6uLfoRmmQydERnKv9j6ew1ogGA3FuAkLemq4USXfTXY=";
        };

        devCmakePrelude = pkgs.writeText "nes-dev-prelude.cmake" ''
          find_package(gflags CONFIG REQUIRED)
          if (NOT TARGET gflags_shared AND TARGET gflags::gflags_shared)
            add_library(gflags_shared ALIAS gflags::gflags_shared)
          endif ()
          find_package(glog CONFIG REQUIRED)
          add_compile_definitions(GLOG_USE_GLOG_EXPORT=1)
        '';

        antlr4JarPatch = pkgs.writeText "nes-antlr-jar.patch" ''
          diff --git a/nes-sql-parser/CMakeLists.txt b/nes-sql-parser/CMakeLists.txt
          --- a/nes-sql-parser/CMakeLists.txt
          +++ b/nes-sql-parser/CMakeLists.txt
          @@ -26,3 +26,5 @@
          -  set(ANTLR4_JAR_LOCATION "/opt/''${ANTLR_JAR_FILE}")
          -else ()
          -  include(FetchContent)
          +  set(ANTLR4_JAR_LOCATION "/opt/''${ANTLR_JAR_FILE}")
          +elseif (DEFINED ANTLR4_JAR_LOCATION)
          +  message(STATUS "Using provided ANTLR jar")
          +else ()
          +  include(FetchContent)
        '';

        gflagsGlogPatch = pkgs.writeText "nes-gflags-glog.patch" ''
diff --git a/CMakeLists.txt b/CMakeLists.txt
index bb54f83a14..06be7b5266 100644
--- a/CMakeLists.txt
+++ b/CMakeLists.txt
@@ -186,6 +186,14 @@ endif ()
 find_package(gRPC CONFIG REQUIRED)
 message(STATUS "Using gRPC ''${gRPC_VERSION}")

+find_package(gflags CONFIG REQUIRED)
+if (NOT TARGET gflags_shared AND TARGET gflags::gflags_shared)
+    add_library(gflags_shared ALIAS gflags::gflags_shared)
+endif ()
+
+find_package(glog CONFIG REQUIRED)
+add_compile_definitions(GLOG_USE_GLOG_EXPORT=1)
+
 set(_GRPC_GRPCPP gRPC::grpc++)
 if (CMAKE_CROSSCOMPILING)
 find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
        '';

        libdwarfModule = pkgs.writeTextFile {
          name = "libdwarf-cmake";
          destination = "/share/cmake/Modules/Findlibdwarf.cmake";
          text = ''
            set(libdwarf_INCLUDE_DIR "${pkgs.libdwarf.dev}/include/libdwarf-2")
            find_library(libdwarf_LIBRARY
              NAMES dwarf libdwarf
              PATHS
                "${(pkgs.libdwarf.lib or pkgs.libdwarf)}/lib"
                "${pkgs.libdwarf}/lib"
            )
            include(FindPackageHandleStandardArgs)
            find_package_handle_standard_args(libdwarf DEFAULT_MSG libdwarf_INCLUDE_DIR libdwarf_LIBRARY)
            if(libdwarf_FOUND)
              add_library(libdwarf::libdwarf UNKNOWN IMPORTED)
              set_target_properties(libdwarf::libdwarf PROPERTIES
                IMPORTED_LOCATION ''${libdwarf_LIBRARY}
                INTERFACE_INCLUDE_DIRECTORIES ''${libdwarf_INCLUDE_DIR})
              set(libdwarf_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
              set(LIBDWARF_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
              set(libdwarf_LIBRARIES libdwarf::libdwarf)
              set(LIBDWARF_LIBRARIES libdwarf::libdwarf)
            endif()
          '';
        };

        antlr4 = clangStdenv.mkDerivation rec {
          pname = "antlr4";
          version = antlr4Version;

          src = pkgs.fetchFromGitHub {
            owner = "antlr";
            repo = "antlr4";
            rev = antlr4Version;
            hash = "sha512-dSzBW7T1Qo8S1StnIsRfW3dcXNdGYI19KZPtVX2sY6Uyd5j4mwfwXu5n/ncDaUd3Fs8KcjcLUNrXWxCZTEdTdg==";
          };

          sourceRoot = "source/runtime/Cpp";

          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [
            pkgs.libuuid
            pkgs.zlib
          ];

          postPatch = ''
            substituteInPlace runtime/CMakeLists.txt \
              --replace "target_compile_definitions(antlr4_shared PUBLIC ANTLR4CPP_EXPORTS)" "target_compile_definitions(antlr4_shared PRIVATE ANTLR4CPP_EXPORTS)"
            substituteInPlace runtime/src/atn/ProfilingATNSimulator.cpp \
              --replace "#include \"atn/ProfilingATNSimulator.h\"" "#include \"atn/ProfilingATNSimulator.h\"\n#include <chrono>"
          '';

          cmakeFlags = [
            "-G"
            "Ninja"
            "-DANTLR_BUILD_STATIC=ON"
            "-DANTLR_BUILD_SHARED=ON"
            "-DANTLR4_INSTALL=ON"
            "-DANTLR_BUILD_CPP_TESTS=OFF"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          meta = with lib; {
            description = "ANTLR4 C++ runtime for NebulaStream";
            homepage = "https://www.antlr.org";
            license = licenses.bsd3;
            platforms = platforms.linux;
          };
        };

        cpptrace = clangStdenv.mkDerivation rec {
          pname = "cpptrace";
          version = "0.8.3";

          src = pkgs.fetchFromGitHub {
            owner = "jeremy-rifkin";
            repo = "cpptrace";
            rev = "v${version}";
            hash = "sha512-T+fmn1DvgAhUBjanRJBcXc3USAJe4Qs2v5UpiLj+HErLtRKoOCr9V/Pa5Nfpfla9v5H/q/2REKpBJ3r4exSSoQ==";
          };

          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [
            pkgs.libdwarf.dev
            pkgs.zstd.dev
          ];

          cmakeFlags = [
            "-G"
            "Ninja"
            "-DCMAKE_BUILD_TYPE=Release"
            "-DCPPTRACE_USE_EXTERNAL_LIBDWARF=ON"
            "-DCPPTRACE_USE_EXTERNAL_ZSTD=ON"
            "-DCPPTRACE_BUILD_TESTS=OFF"
            "-DCPPTRACE_BUILD_EXAMPLES=OFF"
            "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          meta = with lib; {
            description = "C++ stack trace library";
            homepage = "https://github.com/jeremy-rifkin/cpptrace";
            license = licenses.mit;
            platforms = platforms.linux;
          };
        };

        argparse = clangStdenv.mkDerivation rec {
          pname = "argparse";
          version = "3.2";

          src = pkgs.fetchFromGitHub {
            owner = "p-ranav";
            repo = "argparse";
            rev = "v${version}";
            hash = "sha512-TGSBdWib2WSlIZp8hqYVC4YzcZDaGICPLlUfxg1XFIUAyV4v1L3gKxKtiy1w0aTCdRxFZwt++mnEdv4AX+zjdw==";
          };

          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [ ];

          cmakeFlags = [
            "-G"
            "Ninja"
            "-DARGPARSE_BUILD_TESTS=OFF"
            "-DARGPARSE_INSTALL_CMAKE_DIR=lib/cmake/argparse"
            "-DARGPARSE_INSTALL_PKGCONFIG_DIR=lib/pkgconfig"
            "-DCMAKE_INSTALL_INCLUDEDIR=include"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          meta = with lib; {
            description = "Header-only C++17 argument parser";
            homepage = "https://github.com/p-ranav/argparse";
            license = licenses.mit;
            platforms = platforms.linux;
          };
        };

        libcuckoo = clangStdenv.mkDerivation rec {
          pname = "libcuckoo";
          version = "0.3.1";

          src = pkgs.fetchFromGitHub {
            owner = "efficient";
            repo = "libcuckoo";
            rev = "ea8c36c65bf9cf83aaf6b0db971248c6ae3686cf";
            hash = "sha512-kXfaPFyDC2x4osXD81zglOXIQG6CHxts2thp1V9FxGMdR+7kLGBJPtBoIMzPNR2biu3/xSpVlkXkM8Megh1gxA==";
          };

          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [ ];

          cmakeFlags = [
            "-G"
            "Ninja"
            "-DBUILD_EXAMPLES=OFF"
            "-DBUILD_TESTS=OFF"
            "-DBUILD_STRESS_TESTS=OFF"
            "-DBUILD_UNIT_TESTS=OFF"
            "-DBUILD_UNIVERSAL_BENCHMARK=OFF"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          meta = with lib; {
            description = "C++ library providing fast concurrent hash tables";
            homepage = "https://github.com/efficient/libcuckoo";
            license = licenses.bsd3;
            platforms = platforms.linux;
          };
        };

        nautilusSrc = pkgs.fetchFromGitHub {
          owner = "nebulastream";
          repo = "nautilus";
          rev = "364c89c184430e1ccd0d9422fc73b1cc54c33ca8";
          hash = "sha512-yMC9HCBRd3yQuLWXKYCurbEUEb+2OzQa3E3o/MtjfrqcoMTJfqXI/e4jDRdk6V6ehhqSqfeyGGuAuRA558QwsQ==";
        };

        nautilusPkg = clangStdenv.mkDerivation rec {
          pname = "nautilus";
          version = "0.1";

          src = nautilusSrc;

          nativeBuildInputs = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
          ];
          buildInputs = [
            mlirBinary
            llvm.llvm
            fmtPkg
            spdlogPkg
            pkgs.binutils
            pkgs.elfutils
            pkgs.libdwarf.dev
            pkgs.zlib.dev
            pkgs.libffi
            pkgs.libxml2
            pkgs.boost
            pkgs.openssl.dev
            pkgs.tbb
            pkgs.python3
          ];

          CMAKE_CXX_FLAGS = "-Wno-error=unused-result";

          postPatch = ''
            substituteInPlace nautilus/src/nautilus/compiler/backends/mlir/MLIRCompilationBackend.cpp \
              --replace "context.allowsUnregisteredDialects();" "(void)context.allowsUnregisteredDialects();"
          '';

          cmakeFlags = [
            "-G"
            "Ninja"
            "-DCMAKE_BUILD_TYPE=Release"
            "-DUSE_EXTERNAL_FMT=ON"
            "-DUSE_EXTERNAL_SPDLOG=ON"
            "-DUSE_EXTERNAL_MLIR=ON"
            "-DNAUTILUS_DOWNLOAD_MLIR=OFF"
            "-DENABLE_LOGGING=ON"
            "-DENABLE_STACKTRACE=OFF"
            "-DENABLE_COMPILER=ON"
            "-DENABLE_TRACING=ON"
            "-DENABLE_MLIR_BACKEND=ON"
            "-DENABLE_C_BACKEND=ON"
            "-DENABLE_BC_BACKEND=OFF"
            "-DENABLE_TESTS=OFF"
            "-DMLIR_DIR=${commonCmakeEnv.MLIR_DIR}"
            "-DLLVM_DIR=${commonCmakeEnv.LLVM_DIR}"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          meta = with lib; {
            description = "NebulaStream Nautilus compiler";
            homepage = "https://github.com/nebulastream/nautilus";
            license = licenses.asl20;
            platforms = platforms.linux;
          };
        };

        thirdPartyDeps = baseThirdPartyDeps ++ [
          cpptrace
          argparse
          libcuckoo
          nautilusPkg
        ];

        cmakeInputs = [ mlirBinary libdwarfModule ] ++ thirdPartyDeps;

        cmakePrefixPath = lib.makeSearchPath "" cmakeInputs;

        pkgConfigPath = lib.concatStringsSep ":" (
          map (dir: lib.makeSearchPath dir thirdPartyDeps) [
            "lib/pkgconfig"
            "share/pkgconfig"
          ]
        );

        commonCmakeEnv = {
          CMAKE_PREFIX_PATH = cmakePrefixPath;
          PKG_CONFIG_PATH = pkgConfigPath;
          MLIR_DIR = "${mlirBinary}/lib/cmake/mlir";
          LLVM_DIR = "${mlirBinary}/lib/cmake/llvm";
          CMAKE_MODULE_PATH = lib.makeSearchPath "share/cmake/Modules" [ libdwarfModule ];
        };

        # Core development tools
        buildTools = with pkgs; [
          cmake
          ninja
          pkg-config
          git
        ];

        # LLVM 19 toolchain with versioned symlinks for vcpkg
        clangWithVersions = pkgs.symlinkJoin {
          name = "clang-with-versions";
          paths = [ llvm.clang ];
          postBuild = ''
            ln -s $out/bin/clang $out/bin/clang-19
            ln -s $out/bin/clang++ $out/bin/clang++-19
          '';
        };

        # LLVM 19 toolchain
        llvmTools = [
          llvm.llvm
          clangWithVersions
          llvm.lld
          llvm.libcxx
          llvm.clang-tools # clang-format, clang-tidy
        ];

        # Development tools
        devTools = with pkgs; [
          gdb
          llvm.lldb
          python3
        ];

        defaultPackage = clangStdenv.mkDerivation rec {
          pname = "nebulastream";
          version = "unstable";
          src = ./.;

          nativeBuildInputs = buildTools;
          buildInputs = llvmTools ++ thirdPartyDeps ++ [ mlirBinary ];
          patches = [ antlr4JarPatch gflagsGlogPatch ];

          CMAKE_PREFIX_PATH = cmakePrefixPath;
          PKG_CONFIG_PATH = pkgConfigPath;

          postPatch = ''
            substituteInPlace CMakeLists.txt --replace "find_package(Protobuf REQUIRED)" "find_package(Protobuf CONFIG REQUIRED)"
          '';

          cmakeFlags = [
            "-DCMAKE_BUILD_TYPE=Release"
            "-DUSE_LOCAL_MLIR=ON"
            "-DUSE_LIBCXX_IF_AVAILABLE=OFF"
            "-DNES_USE_SYSTEM_DEPS=ON"
            "-DLLVM_TOOLCHAIN_VERSION=19"
            "-DMLIR_DIR=${commonCmakeEnv.MLIR_DIR}"
            "-DLLVM_DIR=${commonCmakeEnv.LLVM_DIR}"
            "-DNES_ENABLES_TESTS=ON"
            "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
            "-DANTLR4_JAR_LOCATION=${antlr4Jar}"
          ];

          enableParallelBuilding = true;
          strictDeps = true;

          installPhase = ''
            runHook preInstall

            mkdir -p $out/bin
            for binary in \
              nes-single-node-worker/nes-single-node-worker \
              nes-nebuli/nes-nebuli \
              nes-nebuli/nes-nebuli-embedded \
              nes-systests/systest/systest
            do
              if [ -x "$binary" ]; then
                install -Dm755 "$binary" "$out/bin/$(basename "$binary")"
              fi
            done

            mkdir -p $out/lib
            find nes-* -maxdepth 1 -type f \( -name 'lib*.a' -o -name 'lib*.so' -o -name 'lib*.so.*' \) \
              -exec sh -c 'for lib; do install -Dm644 "$lib" "$out/lib/$(basename "$lib")"; done' sh {} +

            runHook postInstall
          '';
        };

      in
      {
        formatter = pkgs.nixfmt-tree;

        packages.default = defaultPackage;

        checks = {
          antlr4 = antlr4;
          cpptrace = cpptrace;
          nautilus = nautilusPkg;
        };

        devShells.default = mkShellClang (
          commonCmakeEnv
          // {
            name = "nebula-stream";
            buildInputs = thirdPartyDeps ++ [ mlirBinary ];
            nativeBuildInputs = buildTools ++ llvmTools;
            packages = devTools;
            LLVM_TOOLCHAIN_VERSION = "19";
            CMAKE_GENERATOR = "Ninja";
            VCPKG_ENV_PASSTHROUGH = "MLIR_DIR;LLVM_DIR;CMAKE_PREFIX_PATH";
            NES_USE_SYSTEM_DEPS = "ON";
            cmakeFlags = [
              "-DCMAKE_PROJECT_INCLUDE=${devCmakePrelude}"
              "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
              "-DNES_USE_SYSTEM_DEPS=ON"
              "-DUSE_LOCAL_MLIR=ON"
              "-DUSE_LIBCXX_IF_AVAILABLE=OFF"
              "-DLLVM_TOOLCHAIN_VERSION=19"
              "-DMLIR_DIR=${commonCmakeEnv.MLIR_DIR}"
              "-DLLVM_DIR=${commonCmakeEnv.LLVM_DIR}"
              "-DANTLR4_JAR_LOCATION=${antlr4Jar}"
              "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
            ];
            shellHook = ''
              unset NES_PREBUILT_VCPKG_ROOT
            '';
          }
        );
      }
    );
}
