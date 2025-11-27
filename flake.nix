# Main nix build file for package (`nix build`) and development shell (`nix development`)
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
        # General definitions
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        llvm = pkgs.llvmPackages_19;
        clangStdenv = llvm.stdenv;
        libcxxStdenv = llvm.libcxxStdenv;
        stdenvForStdlib = stdlib: if stdlib.name == "libcxx" then llvm.libcxxStdenv else clangStdenv;
        mkShellForStdlib = stdlib: pkgs.mkShell.override { stdenv = stdenvForStdlib stdlib; };

        # Options for specifying sanitizers
        sanitizerOptions = {
          none = {
            name = "none";
            cmakeValue = "none";
            envValue = "none";
            extraEnv = { };
            extraPackages = [ ];
            compilerFlags = [ ];
            linkerFlags = [ ];
          };
          address = {
            name = "asan";
            cmakeValue = "address";
            envValue = "address";
            extraEnv = {
              ASAN_OPTIONS = "detect_leaks=1";
            };
            extraPackages = [
              llvm.compiler-rt
              llvm.compiler-rt.dev
            ];
            compilerFlags = [
              "-fsanitize=address"
              "-fno-omit-frame-pointer"
            ];
            linkerFlags = [ "-fsanitize=address" ];
          };
          thread = {
            name = "tsan";
            cmakeValue = "thread";
            envValue = "thread";
            extraEnv = {
              TSAN_OPTIONS = "halt_on_error=1";
            };
            extraPackages = [
              llvm.compiler-rt
              llvm.compiler-rt.dev
            ];
            compilerFlags = [ "-fsanitize=thread" ];
            linkerFlags = [ "-fsanitize=thread" ];
          };
          undefined = {
            name = "ubsan";
            cmakeValue = "undefined";
            envValue = "undefined";
            extraEnv = {
              UBSAN_OPTIONS = "print_stacktrace=1";
            };
            extraPackages = [
              llvm.compiler-rt
              llvm.compiler-rt.dev
            ];
            compilerFlags = [ "-fsanitize=undefined" ];
            linkerFlags = [ "-fsanitize=undefined" ];
          };
        };

        sanitizerNames = builtins.attrNames sanitizerOptions;

        # Options for standard library
        stdlibOptions = {
          libstdcxx = {
            name = "libstdcxx";
            cmakeValue = "OFF";
            envValue = "libstdcxx";
            extraEnv = { };
            extraPackages = [ pkgs.stdenv.cc.cc.lib ];
          };
          libcxx = {
            name = "libcxx";
            cmakeValue = "ON";
            envValue = "libcxx";
            extraEnv = { };
            extraPackages = [
              llvm.libcxx
              pkgs.stdenv.cc.cc.lib
            ];
          };
        };

        stdlibNames = builtins.attrNames stdlibOptions;
        defaultStdlibName = "libstdcxx";

        # Map inputs to third-party libs
        packagesForStdlib =
          {
            stdlib,
            extraInputs ? [ ],
            sanitizer ? sanitizerOptions.none,
          }:
          let
            useLibcxx = stdlib.name == "libcxx";
            overrideStdenv = pkg: if useLibcxx then pkg.override { stdenv = libcxxStdenv; } else pkg;
            fmtPkg = overrideStdenv pkgs.fmt_11;
            spdlogPkg = spdlogPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx fmtPkg;
              compilerFlags = sanitizer.compilerFlags;
              linkerFlags = sanitizer.linkerFlags;
            };
            gflagsPkg = overrideStdenv pkgs.gflags;
            glogBase = pkgs.glog.override (
              {
                gflags = gflagsPkg;
                gtest = gtestPkg;
              }
              // lib.optionalAttrs useLibcxx { stdenv = libcxxStdenv; }
            );
            glogPkg = if useLibcxx then glogBase.overrideAttrs (old: old // { doCheck = false; }) else glogBase;
            abseilPkg = abseilPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
              compilerFlags = sanitizer.compilerFlags;
            };
            protobufPkg = protobufPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
              abseil = abseilPkg;
              compilerFlags = sanitizer.compilerFlags;
              linkerFlags = sanitizer.linkerFlags;
            };
            gtestPkg = overrideStdenv pkgs.gtest;
            re2Pkg = re2Packages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
              abseil = abseilPkg;
              gtest = gtestPkg;
              compilerFlags = sanitizer.compilerFlags;
              linkerFlags = sanitizer.linkerFlags;
            };
            grpcPkg = grpcPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
              protobuf = protobufPkg;
              abseil = abseilPkg;
              re2 = re2Pkg;
              compilerFlags = sanitizer.compilerFlags;
              linkerFlags = sanitizer.linkerFlags;
            };
            yamlPkg = overrideStdenv pkgs.yaml-cpp;
            replxxPkg = overrideStdenv pkgs.replxx;
            magicEnumPkg = pkgs.magic-enum;
            boostPkg = overrideStdenv pkgs.boost;
            tbbPkg = overrideStdenv pkgs.tbb;
            follyPkg = follyPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx fmtPkg;
              compilerFlags = sanitizer.compilerFlags;
              linkerFlags = sanitizer.linkerFlags;
            };
            antlr4Pkg = antlr4Packages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            cpptracePkg = cpptracePackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            argparsePkg = argparsePackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            libcuckooPkg = libcuckooPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            nlohmannJsonPkg = nlohmannJsonPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            baseThirdPartyDeps = [
              fmtPkg
              spdlogPkg
              grpcPkg
              protobufPkg
              abseilPkg
              yamlPkg
              replxxPkg
              magicEnumPkg
              boostPkg
              gflagsPkg
              glogPkg
              gtestPkg
              tbbPkg
              follyPkg
              re2Pkg
              antlr4Pkg
              cpptracePkg
              argparsePkg
              libcuckooPkg
              nlohmannJsonPkg
            ]
            ++ [
              pkgs.openssl.dev
              pkgs.zstd.dev
              pkgs.zlib.dev
              pkgs.libdwarf.dev
              pkgs.libffi
              pkgs.libxml2
              pkgs.python3
              pkgs.openjdk21
              pkgs.libuuid
            ];
          in
          {
            inherit
              fmtPkg
              spdlogPkg
              follyPkg
              baseThirdPartyDeps
              ;
          };

        # Load package files
        nlohmannJsonPackages = pkgs.callPackage ./.nix/nlohmann_json/package.nix { };
        abseilPackages = pkgs.callPackage ./.nix/abseil/package.nix { };
        re2Packages = pkgs.callPackage ./.nix/re2/package.nix { };
        protobufPackages = pkgs.callPackage ./.nix/protobuf/package.nix { };
        grpcPackages = pkgs.callPackage ./.nix/grpc/package.nix { };
        antlr4Packages = pkgs.callPackage ./.nix/antlr4/package.nix { };
        cpptracePackages = pkgs.callPackage ./.nix/cpptrace/package.nix { };
        argparsePackages = pkgs.callPackage ./.nix/argparse/package.nix { };
        libcuckooPackages = pkgs.callPackage ./.nix/libcuckoo/package.nix { };
        spdlogPackages = pkgs.callPackage ./.nix/spdlog/package.nix { };
        follyPackages = pkgs.callPackage ./.nix/folly/package.nix { };
        mlirPackages = import ./.nix/mlir/package.nix { inherit pkgs; };

        mlirBinaryFor = cfg: mlirPackages.forOptions cfg;
        nautilusPackagesFor = mlirBinary: import ./.nix/nautilus/package.nix { inherit pkgs mlirBinary; };

        mkThirdPartyDeps =
          combo:
          let
            sanitizer = combo.sanitizer;
            stdlib = combo.stdlib;
            mlirBinary = combo.mlirBinary;
            extraInputs = sanitizer.extraPackages ++ stdlib.extraPackages;
            packageSet = packagesForStdlib {
              inherit stdlib extraInputs sanitizer;
            };
            baseThirdPartyDeps = packageSet.baseThirdPartyDeps;
            baseWithDev = lib.unique (baseThirdPartyDeps ++ map lib.getDev baseThirdPartyDeps);
            useLibcxx = stdlib.name == "libcxx";
            nautilusPackages = nautilusPackagesFor mlirBinary;
            sanitizedNautilus = nautilusPackages.withSanitizer {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
          in
          baseWithDev ++ [ sanitizedNautilus ];

        mkCmakeContext =
          combo:
          let
            sanitizer = combo.sanitizer;
            stdlib = combo.stdlib;
            mlirBinary = combo.mlirBinary;
            thirdPartyDeps = mkThirdPartyDeps combo;
            cmakeInputs = [
              mlirBinary
              libdwarfModule
            ]
            ++ thirdPartyDeps;
            cmakePrefixPath = lib.makeSearchPath "" cmakeInputs;
            pkgConfigPath = lib.concatStringsSep ":" (
              map (dir: lib.makeSearchPath dir thirdPartyDeps) [
                "lib/pkgconfig"
                "share/pkgconfig"
              ]
            );
            includeRoots = lib.unique (
              map (dep: "${dep}/include") (
                thirdPartyDeps ++ sanitizer.extraPackages ++ stdlib.extraPackages ++ [ mlirBinary ]
              )
            );
            includePath = lib.concatStringsSep ":" includeRoots;
            includePathCMake = lib.concatStringsSep ";" includeRoots;
            libraryRoots = lib.unique (
              map (dep: "${dep}/lib") (
                thirdPartyDeps ++ sanitizer.extraPackages ++ stdlib.extraPackages ++ [ mlirBinary ]
              )
            );
            libraryPath = lib.concatStringsSep ":" libraryRoots;
            includeFlags = lib.concatStringsSep " " (map (path: "-isystem ${path}") includeRoots);
            libcxxLinkerFlags = lib.optionals (stdlib.name == "libcxx") [
              "-L${llvm.libcxx}/lib"
              "-L${pkgs.stdenv.cc.cc.lib}/lib"
              "-lc++abi"
              "-Wl,--no-as-needed"
              "-lstdc++"
              "-Wl,--as-needed"
            ];
            linkerFlags = lib.concatStringsSep " " libcxxLinkerFlags;
            baseEnv = rec {
              CMAKE_PREFIX_PATH = cmakePrefixPath;
              PKG_CONFIG_PATH = pkgConfigPath;
              MLIR_DIR = "${mlirBinary}/lib/cmake/mlir";
              LLVM_DIR = "${mlirBinary}/lib/cmake/llvm";
              CMAKE_MODULE_PATH = lib.makeSearchPath "share/cmake/Modules" [ libdwarfModule ];
              CPATH = includePath;
              CPLUS_INCLUDE_PATH = CPATH;
              C_INCLUDE_PATH = CPATH;
              LIBRARY_PATH = libraryPath;
              CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES = includePathCMake;
            };
            extraEnv = lib.optionalAttrs (linkerFlags != "") {
              LDFLAGS = linkerFlags;
              CMAKE_EXE_LINKER_FLAGS = linkerFlags;
              CMAKE_SHARED_LINKER_FLAGS = linkerFlags;
              CMAKE_MODULE_LINKER_FLAGS = linkerFlags;
            };
            commonCmakeEnv = baseEnv // extraEnv;
          in
          {
            inherit
              thirdPartyDeps
              cmakeInputs
              cmakePrefixPath
              pkgConfigPath
              commonCmakeEnv
              ;
          };

        cmakeCommonFlags = combo: [
          "-DUSE_LOCAL_MLIR=ON"
          "-DUSE_LIBCXX_IF_AVAILABLE=${combo.stdlib.cmakeValue}"
          "-DNES_USE_SYSTEM_DEPS=ON"
          "-DLLVM_TOOLCHAIN_VERSION=19"
          "-DMLIR_DIR=${combo.mlirBinary}/lib/cmake/mlir"
          "-DLLVM_DIR=${combo.mlirBinary}/lib/cmake/llvm"
          "-DANTLR4_JAR_LOCATION=${antlr4Jar}"
          "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
          "-DUSE_SANITIZER=${combo.sanitizer.cmakeValue}"
        ];

        mkVariantAttrset =
          builder:
          let
            combos = lib.listToAttrs (
              lib.concatMap (
                sanName:
                let
                  sanitizer = sanitizerOptions.${sanName};
                in
                map (
                  stdName:
                  let
                    stdlib = stdlibOptions.${stdName};
                    comboName = "${sanitizer.name}-${stdlib.name}";
                  in
                  {
                    name = comboName;
                    value = builder {
                      sanitizerName = sanName;
                      stdlibName = stdName;
                      inherit sanitizer stdlib;
                    };
                  }
                ) stdlibNames
              ) sanitizerNames
            );
            addAliases = lib.foldl' (
              acc: sanName:
              let
                sanitizer = sanitizerOptions.${sanName};
                defaultStdlib = stdlibOptions.${defaultStdlibName};
                defaultKey = "${sanitizer.name}-${defaultStdlib.name}";
                defaultValue = acc.${defaultKey};
              in
              acc // { ${sanName} = defaultValue; } // { ${sanitizer.name} = defaultValue; }
            ) combos sanitizerNames;
          in
          addAliases;

        antlr4Version = "4.13.2";
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

        # Apply NebulaStream specific patches
        patchFilesFor =
          package:
          let
            packageDir = ".nix/${package}/patches";
          in
          lib.optionals (builtins.pathExists packageDir) (
            let
              entries = builtins.readDir packageDir;
              patchNames = lib.filter (name: lib.strings.hasSuffix ".patch" name) (
                lib.attrNames (lib.filterAttrs (_: type: type == "regular") entries)
              );
            in
            map (name: packageDir + "/${name}") patchNames
          );
        nebulastreamPatches = patchFilesFor "nebulastream";

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

        # Core development tools
        buildTools = with pkgs; [
          cmake
          ninja
          pkg-config
          git
          ccache
        ];

        # LLVM 19 toolchain with versioned symlinks for vcpkg
        clangWithVersionsDefault = pkgs.symlinkJoin {
          name = "clang-with-versions";
          paths = [ llvm.clang ];
          postBuild = ''
            ln -s $out/bin/clang $out/bin/clang-19
            ln -s $out/bin/clang++ $out/bin/clang++-19
          '';
        };

        clangWithVersionsLibcxx = pkgs.symlinkJoin {
          name = "clang-with-versions-libcxx";
          paths = [ llvm.libcxxClang ];
          postBuild = ''
            ln -s $out/bin/clang $out/bin/clang-19
            ln -s $out/bin/clang++ $out/bin/clang++-19
          '';
        };

        # LLVM 19 toolchain
        toolchainFor =
          stdlib:
          let
            clangWithVersions =
              if stdlib.name == "libcxx" then clangWithVersionsLibcxx else clangWithVersionsDefault;
          in
          {
            inherit clangWithVersions;
            tools = [
              llvm.llvm
              clangWithVersions
              llvm.lld
              llvm.libcxx
              llvm.clang-tools # clang-format, clang-tidy
            ];
          };

        # Development tools
        devTools = with pkgs; [
          gdb
          llvm.lldb
          python3
        ];

        # Install phase triggered for package build
        packageInstallPhase = ''
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

        # Configuration for package build. Triggered via `nix build`.
        mkPackage =
          {
            sanitizerName,
            sanitizer,
            stdlibName,
            stdlib,
          }:
          let
            mlirBinary = mlirBinaryFor {
              sanitizer = sanitizer.cmakeValue;
              stdlib = stdlib.name;
            };
            combo = {
              inherit sanitizer stdlib mlirBinary;
            };
            cmakeCtx = mkCmakeContext combo;
            toolchain = toolchainFor stdlib;
            llvmToolsVariant = toolchain.tools;
            clangWithVersionsPath = toolchain.clangWithVersions;
            cmakeFlagsList = [
              "-DCMAKE_BUILD_TYPE=Release"
              "-DNES_ENABLES_TESTS=ON"
              "-DUSE_CCACHE_IF_AVAILABLE=OFF"
            ]
            ++ cmakeCommonFlags combo;
            envVars =
              sanitizer.extraEnv
              // stdlib.extraEnv
              // {
                NES_SANITIZER = sanitizer.envValue;
                NES_STDLIB = stdlib.envValue;
                NES_USE_LIBCXX = stdlib.cmakeValue;
                CC = "${clangWithVersionsPath}/bin/clang";
                CXX = "${clangWithVersionsPath}/bin/clang++";
              };
          in
          (stdenvForStdlib stdlib).mkDerivation (
            (rec {
              pname = "nebulastream";
              version = "unstable";
              src = ./.;

              nativeBuildInputs = buildTools;
              buildInputs =
                llvmToolsVariant
                ++ cmakeCtx.thirdPartyDeps
                ++ [ mlirBinary ]
                ++ sanitizer.extraPackages
                ++ stdlib.extraPackages;
              patches = nebulastreamPatches;

              CMAKE_PREFIX_PATH = cmakeCtx.cmakePrefixPath;
              PKG_CONFIG_PATH = cmakeCtx.pkgConfigPath;

              cmakeFlags = cmakeFlagsList;

              enableParallelBuilding = true;
              strictDeps = true;
              installPhase = packageInstallPhase;
            })
            // envVars
          );

        # Configuration for development shell. Triggered via nix-cmake, nix-run or `nix develop`
        mkDevShell =
          {
            sanitizerName,
            sanitizer,
            stdlibName,
            stdlib,
          }:
          let
            mlirBinary = mlirBinaryFor {
              sanitizer = sanitizer.cmakeValue;
              stdlib = stdlib.name;
            };
            combo = {
              inherit sanitizer stdlib mlirBinary;
            };
            cmakeCtx = mkCmakeContext combo;
            toolchain = toolchainFor stdlib;
            llvmToolsVariant = toolchain.tools;
            clangWithVersionsPath = toolchain.clangWithVersions;
            cmakeFlagsList = [
              "-DCMAKE_C_COMPILER_LAUNCHER=ccache"
              "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
              "-DCMAKE_PROJECT_INCLUDE=${devCmakePrelude}"
              "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
            ]
            ++ cmakeCommonFlags combo;
            envVars =
              sanitizer.extraEnv
              // stdlib.extraEnv
              // {
                NES_SANITIZER = sanitizer.envValue;
                NES_STDLIB = stdlib.envValue;
                NES_USE_LIBCXX = stdlib.cmakeValue;
                NES_USE_SYSTEM_DEPS = "ON";
                CC = "${clangWithVersionsPath}/bin/clang";
                CXX = "${clangWithVersionsPath}/bin/clang++";
              };
          in
          (mkShellForStdlib stdlib) (
            cmakeCtx.commonCmakeEnv
            // envVars
            // {
              name = "nebula-stream-${sanitizer.name}-${stdlib.name}";
              buildInputs =
                cmakeCtx.thirdPartyDeps ++ [ mlirBinary ] ++ sanitizer.extraPackages ++ stdlib.extraPackages;
              nativeBuildInputs = buildTools ++ llvmToolsVariant;
              packages = devTools;
              LLVM_TOOLCHAIN_VERSION = "19";
              CMAKE_GENERATOR = "Ninja";
              VCPKG_ENV_PASSTHROUGH = "MLIR_DIR;LLVM_DIR;CMAKE_PREFIX_PATH";
              cmakeFlags = cmakeFlagsList;
              shellHook = ''
                unset NES_PREBUILT_VCPKG_ROOT
                export CCACHE_DIR="$PWD/.ccache"
                export CCACHE_BASEDIR="$PWD"
                mkdir -p "$CCACHE_DIR"
                export CMAKE_C_COMPILER_LAUNCHER=ccache
                export CMAKE_CXX_COMPILER_LAUNCHER=ccache
              '';
            }
          );

        # Creates all necessary scripts and links to seamlessly use nix within Clion.
        # Use via `nix run .#clion-setup`
        clionSetupScript = pkgs.writeShellApplication {
          name = "clion-setup";
          runtimeInputs = [ pkgs.coreutils ];
          text = ''
            set -euo pipefail

            project_root="$(pwd -P)"

            target="$project_root/.nix/nix-run.sh"
            if [ ! -x "$target" ]; then
              printf 'clion-setup: expected executable %s/.nix/nix-run.sh\n' "$project_root" >&2
              printf 'clion-setup: run this command from the repository root after fetching the repo.\n' >&2
              exit 1
            fi

            bin_dir="$project_root/.nix"
            if [ ! -d "$bin_dir" ]; then
              printf 'clion-setup: expected directory %s/.nix to exist.\n' "$project_root" >&2
              exit 1
            fi

            if rel_target=$(realpath --relative-to="$bin_dir" "$target" 2>/dev/null); then
              link_target="$rel_target"
            else
              link_target="$target"
            fi

            created_list=""
            for tool in cc c++ clang clang++ ctest ninja gdb; do
              link="$bin_dir/$tool"
              ln -sf "$link_target" "$link"
              created_list="$created_list\n  - $link -> $link_target"
            done

            printf 'clion-setup: shims available in %s\n' "$bin_dir"
            printf '%b\n' "$created_list"
            printf '\nThese binaries re-enter the Nix dev shell on demand; point CLion at them for compilers, ctest, and ninja.\n'
          '';
        };

      in
      let
        packageVariants = mkVariantAttrset mkPackage;
        devShellVariants = mkVariantAttrset mkDevShell;
      in
      {
        formatter = pkgs.nixfmt-tree;

        packages = packageVariants // {
          default = packageVariants.none;
        };

        checks =
          let
            defaultStdlib = stdlibOptions.${defaultStdlibName};
            defaultSanitizer = sanitizerOptions.none;
            extraInputs = defaultSanitizer.extraPackages ++ defaultStdlib.extraPackages;
            useLibcxx = defaultStdlib.name == "libcxx";
            sanitizerArgs = {
              extraBuildInputs = extraInputs;
              inherit useLibcxx;
            };
            defaultMlirBinary = mlirBinaryFor {
              sanitizer = defaultSanitizer.cmakeValue;
              stdlib = defaultStdlib.name;
            };
            nautilusPackages = nautilusPackagesFor defaultMlirBinary;
          in
          {
            antlr4 = antlr4Packages.withSanitizer sanitizerArgs;
            cpptrace = cpptracePackages.withSanitizer sanitizerArgs;
            argparse = argparsePackages.withSanitizer sanitizerArgs;
            libcuckoo = libcuckooPackages.withSanitizer sanitizerArgs;
            nlohmann_json = nlohmannJsonPackages.withSanitizer sanitizerArgs;
            nautilus = nautilusPackages.withSanitizer sanitizerArgs;
          };

        apps = {
          clion-setup = {
            type = "app";
            program = "${clionSetupScript}/bin/clion-setup";
          };
        };

        devShells = devShellVariants // {
          default = devShellVariants.none;
        };
      }
    );
}
