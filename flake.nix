{
  description = "NebulaStream development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        llvm = pkgs.llvmPackages_19;
        clangStdenv = llvm.stdenv;
        mkShellClang = pkgs.mkShell.override { stdenv = clangStdenv; };

        nautilusPackages = import ./.nix/nautilus/package.nix { inherit pkgs; };
        mlirBinary = nautilusPackages.mlirBinary;
        nautilusPkg = nautilusPackages.nautilus;

        nlohmann_jsonPkg = pkgs.callPackage ./.nix/nlohmann_json/package.nix { };
        nameofPkg = pkgs.callPackage ./.nix/nameof/package.nix { };
        scope_guardPkg = pkgs.callPackage ./.nix/scope_guard/package.nix { };
        cpptracePkg = pkgs.callPackage ./.nix/cpptrace/package.nix { };
        argparsePkg = pkgs.callPackage ./.nix/argparse/package.nix { };
        libcuckooPkg = pkgs.callPackage ./.nix/libcuckoo/package.nix { };

        fmtPkg = pkgs.fmt_11;
        spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };
        follyPkg = import ./.nix/folly/package.nix { inherit pkgs; };
        antlr4Pkg = import ./.nix/antlr4/package.nix { inherit pkgs; };
        simdjsonPkg =
          let
            version = "4.0.7";
          in
          pkgs.simdjson.overrideAttrs (_:
            {
              inherit version;
              src = pkgs.fetchFromGitHub {
                owner = "simdjson";
                repo = "simdjson";
                rev = "v${version}";
                hash = "sha256-8pmFtMpML7tTXbH1E3aIpSTQkNF8TFcIPOm2nwnKxkA=";
              };
            });

        baseThirdPartyDeps = (with pkgs; [
          fmtPkg
          spdlogPkg
          simdjsonPkg
          follyPkg
          antlr4Pkg
          grpc
          protobuf
          abseil-cpp
          yaml-cpp
          replxx
          magic-enum
          libuuid
          boost
          openssl.dev
          zstd.dev
          zlib.dev
          libdwarf.dev
          libffi
          libxml2
          gdb
          gflags
          glog
          gtest
          tbb
          python3
          openjdk21
        ]);

        antlr4Jar = pkgs.fetchurl {
          url = "https://www.antlr.org/download/antlr-${antlr4Pkg.version}-complete.jar";
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

        thirdPartyDeps = baseThirdPartyDeps ++ [
          cpptracePkg
          argparsePkg
          libcuckooPkg
          nautilusPkg
          nlohmann_jsonPkg
          nameofPkg
          scope_guardPkg
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

        defaultPackage = clangStdenv.mkDerivation rec {
          pname = "nebulastream";
          version = "unstable";
          src = ./.;

          nativeBuildInputs = buildTools;
          buildInputs = llvmTools ++ thirdPartyDeps ++ [ mlirBinary ];
          patches = nebulastreamPatches;

          CMAKE_PREFIX_PATH = cmakePrefixPath;
          PKG_CONFIG_PATH = pkgConfigPath;

          postPatch = ''
            substituteInPlace CMakeLists.txt --replace "find_package(Protobuf REQUIRED)" "find_package(Protobuf CONFIG REQUIRED)"
          '';

          cmakeFlags = [
            "-DCMAKE_C_COMPILER_LAUNCHER=ccache"
            "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
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
          antlr4 = antlr4Pkg;
          cpptrace = cpptracePkg;
          nautilus = nautilusPkg;
        };

        apps =
          let
            formatRunner = pkgs.writeShellApplication {
              name = "nes-format";
              runtimeInputs =
                [
                  pkgs.git
                  pkgs.coreutils
                  pkgs.findutils
                  pkgs.gnugrep
                  pkgs.gawk
                  pkgs.python3
                  pkgs.util-linux
                ]
                ++ llvmTools;
              text = ''
                set -euo pipefail
                if [ ! -x ./scripts/format.sh ]; then
                  echo "nes-format: run this command from the NebulaStream repository root" >&2
                  exit 1
                fi
                if [ "$#" -gt 0 ]; then
                  echo "nes-format: always runs with -i; ignoring extra arguments: $*" >&2
                fi
                ./scripts/format.sh -i
              '';
            };
          in
          {
            clion-setup = {
              type = "app";
              program = "${clionSetupScript}/bin/clion-setup";
            };
            format = {
              type = "app";
              program = "${formatRunner}/bin/nes-format";
            };
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
              "-DCMAKE_C_COMPILER_LAUNCHER=ccache"
              "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
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
              export CCACHE_DIR="$PWD/.ccache"
              export CCACHE_BASEDIR="$PWD"
              mkdir -p "$CCACHE_DIR"
              export CMAKE_C_COMPILER_LAUNCHER=ccache
              export CMAKE_CXX_COMPILER_LAUNCHER=ccache
            '';
          }
        );
      }
    );
}
