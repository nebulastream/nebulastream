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

        sanitizerOptions = {
          none = {
            name = "none";
            cmakeValue = "none";
            envValue = "none";
            extraEnv = { };
            extraPackages = [ ];
          };
          address = {
            name = "asan";
            cmakeValue = "address";
            envValue = "address";
            extraEnv = {
              ASAN_OPTIONS = "detect_leaks=1";
            };
            extraPackages = [ llvm.compiler-rt ];
          };
          thread = {
            name = "tsan";
            cmakeValue = "thread";
            envValue = "thread";
            extraEnv = {
              TSAN_OPTIONS = "halt_on_error=1";
            };
            extraPackages = [ llvm.compiler-rt ];
          };
          undefined = {
            name = "ubsan";
            cmakeValue = "undefined";
            envValue = "undefined";
            extraEnv = {
              UBSAN_OPTIONS = "print_stacktrace=1";
            };
            extraPackages = [ llvm.compiler-rt ];
          };
        };

        sanitizerNames = builtins.attrNames sanitizerOptions;

        antlr4Version = "4.13.2";

        fmtPkg = pkgs.fmt_11;
        spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };
        follyPkg = import ./.nix/folly/package.nix { inherit pkgs; };
        nlohmannJsonPackages = pkgs.callPackage ./.nix/nlohmann_json/package.nix { };

        coreThirdPartyDeps = (with pkgs; [
          fmtPkg
          spdlogPkg
          grpc
          protobuf
          abseil-cpp
          yaml-cpp
          replxx
          magic-enum
          gdb
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
        ]);

        ccacheFlags = [
          "-DCMAKE_C_COMPILER_LAUNCHER=ccache"
          "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
        ];

        ccacheShellHook = ''
          export CCACHE_DIR="$PWD/.ccache"
          export CCACHE_BASEDIR="$PWD"
          mkdir -p "$CCACHE_DIR"
          export CMAKE_C_COMPILER_LAUNCHER=ccache
          export CMAKE_CXX_COMPILER_LAUNCHER=ccache
        '';

        antlr4Packages = pkgs.callPackage ./.nix/antlr4/package.nix { };
        cpptracePackages = pkgs.callPackage ./.nix/cpptrace/package.nix { };
        argparsePackages = pkgs.callPackage ./.nix/argparse/package.nix { };
        libcuckooPackages = pkgs.callPackage ./.nix/libcuckoo/package.nix { };

        mlirPackages = import ./.nix/mlir/package.nix { inherit pkgs; };
        mlirBinary = mlirPackages.mlirBinary;

        nautilusPackages = import ./.nix/nautilus/package.nix {
          inherit pkgs mlirBinary;
        };

        sanitizerPackageSet = {
          antlr4 = antlr4Packages;
          cpptrace = cpptracePackages;
          argparse = argparsePackages;
          libcuckoo = libcuckooPackages;
          nautilus = nautilusPackages;
          nlohmann_json = nlohmannJsonPackages;
        };

        sanitizerPackages = lib.attrValues sanitizerPackageSet;

        mkThirdPartyDeps = sanitizer:
          coreThirdPartyDeps
          ++ [ follyPkg ]
          ++ map (pkg: pkg.withSanitizer sanitizer.extraPackages) sanitizerPackages;

        mkCmakeContext = sanitizer:
          let
            thirdPartyDeps = mkThirdPartyDeps sanitizer;
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
          in {
            inherit thirdPartyDeps cmakeInputs cmakePrefixPath pkgConfigPath commonCmakeEnv;
          };

        cmakeCommonFlags = sanitizer: [
          "-DUSE_LOCAL_MLIR=ON"
          "-DUSE_LIBCXX_IF_AVAILABLE=OFF"
          "-DNES_USE_SYSTEM_DEPS=ON"
          "-DLLVM_TOOLCHAIN_VERSION=19"
          "-DMLIR_DIR=${mlirBinary}/lib/cmake/mlir"
          "-DLLVM_DIR=${mlirBinary}/lib/cmake/llvm"
          "-DANTLR4_JAR_LOCATION=${antlr4Jar}"
          "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
          "-DUSE_SANITIZER=${sanitizer.cmakeValue}"
        ];

        mkSanitizedAttrset = builder:
          lib.foldl'
            (acc: sanName:
              let
                sanitizer = sanitizerOptions.${sanName};
                value = builder sanName;
              in
              acc
              // { ${sanName} = value; }
              // { ${sanitizer.name} = value; }
            )
            { }
            sanitizerNames;

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

        patchRoot = ./.nix;

        patchFilesFor = package:
          let
            packageDir = patchRoot + "/${package}/patches";
          in
          lib.optionals (builtins.pathExists packageDir) (
            let
              entries = builtins.readDir packageDir;
              patchNames = lib.filter (name: lib.strings.hasSuffix ".patch" name)
                (lib.attrNames (lib.filterAttrs (_: type: type == "regular") entries));
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

        # Core development tools
        buildTools = with pkgs; [
          cmake
          ninja
          pkg-config
          git
          ccache
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

        mkPackage = sanitizerName:
          let
            sanitizer = sanitizerOptions.${sanitizerName};
            cmakeCtx = mkCmakeContext sanitizer;
            cmakeFlagsList = ccacheFlags
              ++ [
                "-DCMAKE_BUILD_TYPE=Release"
                "-DNES_ENABLES_TESTS=ON"
              ]
              ++ cmakeCommonFlags sanitizer;
            envVars = sanitizer.extraEnv // { NES_SANITIZER = sanitizer.envValue; };
          in
          clangStdenv.mkDerivation ((rec {
            pname = "nebulastream";
            version = "unstable";
            src = ./.;

            nativeBuildInputs = buildTools;
            buildInputs = llvmTools ++ cmakeCtx.thirdPartyDeps ++ [ mlirBinary ] ++ sanitizer.extraPackages;
            patches = nebulastreamPatches;

            CMAKE_PREFIX_PATH = cmakeCtx.cmakePrefixPath;
            PKG_CONFIG_PATH = cmakeCtx.pkgConfigPath;

            cmakeFlags = cmakeFlagsList;

            enableParallelBuilding = true;
            strictDeps = true;
            installPhase = packageInstallPhase;
          }) // envVars);

        mkDevShell = sanitizerName:
          let
            sanitizer = sanitizerOptions.${sanitizerName};
            cmakeCtx = mkCmakeContext sanitizer;
            cmakeFlagsList = ccacheFlags
              ++ [
                "-DCMAKE_PROJECT_INCLUDE=${devCmakePrelude}"
                "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON"
              ]
              ++ cmakeCommonFlags sanitizer;
            envVars = sanitizer.extraEnv // {
              NES_SANITIZER = sanitizer.envValue;
              NES_USE_SYSTEM_DEPS = "ON";
            };
          in
          mkShellClang (
            cmakeCtx.commonCmakeEnv
            // envVars
            // {
              name = "nebula-stream-${sanitizer.name}";
              buildInputs = cmakeCtx.thirdPartyDeps ++ [ mlirBinary ] ++ sanitizer.extraPackages;
              nativeBuildInputs = buildTools ++ llvmTools;
              packages = devTools;
              LLVM_TOOLCHAIN_VERSION = "19";
              CMAKE_GENERATOR = "Ninja";
              VCPKG_ENV_PASSTHROUGH = "MLIR_DIR;LLVM_DIR;CMAKE_PREFIX_PATH";
              cmakeFlags = cmakeFlagsList;
              shellHook = ''
                unset NES_PREBUILT_VCPKG_ROOT
              '' + ccacheShellHook;
            }
          );

      in
      let
        packageVariants = mkSanitizedAttrset mkPackage;
        devShellVariants = mkSanitizedAttrset mkDevShell;
      in
      {
        formatter = pkgs.nixfmt-tree;

        packages = packageVariants // { default = packageVariants.none; };

        checks = {
          antlr4 = sanitizerPackageSet.antlr4.withSanitizer sanitizerOptions.none.extraPackages;
          cpptrace = sanitizerPackageSet.cpptrace.withSanitizer sanitizerOptions.none.extraPackages;
          nautilus = sanitizerPackageSet.nautilus.withSanitizer sanitizerOptions.none.extraPackages;
        };

        apps = {
          clion-setup = {
            type = "app";
            program = "${clionSetupScript}/bin/clion-setup";
          };
        };

        devShells = devShellVariants // { default = devShellVariants.none; };
      }
    );
}
