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
            compileFlags = [ ];
            linkFlags = [ ];
          };
          address = {
            name = "asan";
            cmakeValue = "address";
            envValue = "address";
            extraEnv = {
              ASAN_OPTIONS = "detect_leaks=1";
            };
            extraPackages = [ llvm.compiler-rt ];
            compileFlags = [ "-fsanitize=address" ];
            linkFlags = [ "-fsanitize=address" ];
          };
          thread = {
            name = "tsan";
            cmakeValue = "thread";
            envValue = "thread";
            extraEnv = {
              TSAN_OPTIONS = "halt_on_error=1";
            };
            extraPackages = [ llvm.compiler-rt ];
            compileFlags = [ "-fsanitize=thread" ];
            linkFlags = [ "-fsanitize=thread" ];
          };
          undefined = {
            name = "ubsan";
            cmakeValue = "undefined";
            envValue = "undefined";
            extraEnv = {
              UBSAN_OPTIONS = "print_stacktrace=1";
            };
            extraPackages = [ llvm.compiler-rt ];
            compileFlags = [ "-fsanitize=undefined" "-fno-sanitize=alignment" ];
            linkFlags = [ "-fsanitize=undefined" "-fno-sanitize=alignment" ];
          };
        };

        sanitizerNames = builtins.attrNames sanitizerOptions;

        antlr4Version = "4.13.2";

        sanitizerSupport = import ./.nix/lib/sanitizer-support.nix { inherit lib; };

        fmtBase = pkgs.fmt_11;
        boostBase = pkgs.boost;
        abseilBase = pkgs.abseil-cpp;
        protobufBase = pkgs.protobuf;
        grpcBase = pkgs.grpc;
        yamlCppBase = pkgs.yaml-cpp;
        replxxBase = pkgs.replxx;
        magicEnumBase = pkgs.magic-enum;
        gtestBase = pkgs.gtest;

        antlr4Base = pkgs.callPackage ./.nix/antlr4/package.nix { };
        cpptraceBase = pkgs.callPackage ./.nix/cpptrace/package.nix { };
        argparseBase = pkgs.callPackage ./.nix/argparse/package.nix { };
        libcuckooBase = pkgs.callPackage ./.nix/libcuckoo/package.nix { };
        follyBase = import ./.nix/folly/package.nix { inherit pkgs; };
        nlohmannJsonBase = pkgs.callPackage ./.nix/nlohmann_json/package.nix { };

        coreThirdPartyDeps = (with pkgs; [
          openssl.dev
          zstd.dev
          zlib.dev
          libdwarf.dev
          libffi
          libxml2
          gdb
          gflags
          glog
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

        mlirPackages = import ./.nix/mlir/package.nix { inherit pkgs; };
        mlirForSanitizer = sanitizer: mlirPackages.withSanitizer sanitizer;

        nautilusPackages = import ./.nix/nautilus/package.nix {
          inherit pkgs mlirForSanitizer;
          mlirDefault = mlirPackages.default;
        };

        mkThirdPartyDeps = sanitizer:
          let
            sanitizeDrv =
              { drv, disableChecks ? false, extraBuildInputs ? [ ] }:
                sanitizerSupport.sanitizePackage {
                  inherit sanitizer drv disableChecks extraBuildInputs;
                };
            fmtPkg = sanitizeDrv { drv = fmtBase; disableChecks = true; };
            spdlogPkg = sanitizeDrv { drv = pkgs.spdlog.override { fmt = fmtPkg; }; };
            boostPkg = sanitizeDrv { drv = boostBase; };
            abseilPkg = sanitizeDrv { drv = abseilBase; };
            protobufPkg = sanitizeDrv { drv = protobufBase; disableChecks = true; };
            grpcPkg = sanitizeDrv { drv = grpcBase; };
            yamlCppPkg = sanitizeDrv { drv = yamlCppBase; };
            replxxPkg = sanitizeDrv { drv = replxxBase; };
            magicEnumPkg = sanitizeDrv { drv = magicEnumBase; };
            follyPkg = sanitizeDrv { drv = follyBase; };
            gtestPkg = sanitizeDrv { drv = gtestBase; disableChecks = true; };
            antlr4Pkg = sanitizeDrv { drv = antlr4Base; };
            cpptracePkg =
              if sanitizer.name == sanitizerOptions.none.name then
                cpptraceBase.default
              else
                cpptraceBase.withSanitizer sanitizer;
            argparsePkg =
              if sanitizer.name == sanitizerOptions.none.name then
                argparseBase.default
              else
                argparseBase.withSanitizer sanitizer;
            libcuckooPkg =
              if sanitizer.name == sanitizerOptions.none.name then
                libcuckooBase.default
              else
                libcuckooBase.withSanitizer sanitizer;
            nlohmannJsonPkg =
              if sanitizer.name == sanitizerOptions.none.name then
                nlohmannJsonBase.default
              else
                nlohmannJsonBase.withSanitizer sanitizer;
            nautilusPkg =
              if sanitizer.name == sanitizerOptions.none.name then
                nautilusPackages.default
              else
                nautilusPackages.withSanitizer sanitizer;
          in
          coreThirdPartyDeps
          ++ [
            fmtPkg
            spdlogPkg
            boostPkg
            protobufPkg
            abseilPkg
            grpcPkg
            yamlCppPkg
            replxxPkg
            magicEnumPkg
            follyPkg
            gtestPkg
            antlr4Pkg
            cpptracePkg
            argparsePkg
            libcuckooPkg
            nautilusPkg
            nlohmannJsonPkg
          ];

        mkCmakeContext = sanitizer:
          let
            mlirBinary = mlirForSanitizer sanitizer;
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
              NES_EXTRA_MODULE_PATH = lib.makeSearchPath "share/cmake/Modules" [ libdwarfModule ];
            };
          in {
            inherit thirdPartyDeps cmakeInputs cmakePrefixPath pkgConfigPath commonCmakeEnv;
          };

        cmakeCommonFlags = sanitizer:
          let
            mlirBinary = mlirForSanitizer sanitizer;
          in [
          "-DUSE_LOCAL_MLIR=ON"
          "-DUSE_LIBCXX_IF_AVAILABLE=OFF"
          "-DNES_USE_SYSTEM_DEPS=ON"
          "-DLLVM_TOOLCHAIN_VERSION=19"
          "-DMLIR_DIR=${mlirBinary}/lib/cmake/mlir"
          "-DLLVM_DIR=${mlirBinary}/lib/cmake/llvm"
          "-DANTLR4_JAR_LOCATION=${antlr4Jar}"
          "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
          "-DUSE_SANITIZER=${sanitizer.cmakeValue}"
        ]
        ++ lib.optionals (sanitizer.name != sanitizerOptions.none.name) [
          "-DLLVM_ENABLE_FFI=OFF"
        ];

        mkSanitizedAttrset = builder:
          builtins.listToAttrs (
            lib.concatMap
              (sanName:
                let
                  sanitizer = sanitizerOptions.${sanName};
                  value = builder sanName;
                  aliasName = sanitizer.name;
                  attrEntries = [
                    {
                      name = sanName;
                      inherit value;
                    }
                  ];
                in
                attrEntries
                ++ lib.optionals (aliasName != sanName) [
                  {
                    name = aliasName;
                    inherit value;
                  }
                ]
              )
              sanitizerNames
          );

        antlr4Jar = pkgs.fetchurl {
          url = "https://www.antlr.org/download/antlr-${antlr4Version}-complete.jar";
          hash = "sha256-6uLfoRmmQydERnKv9j6ew1ogGA3FuAkLemq4USXfTXY=";
        };

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
            mlirBinary = mlirForSanitizer sanitizer;
            cmakeFlagsList = ccacheFlags
              ++ [
                "-DCMAKE_BUILD_TYPE=Release"
                "-DNES_ENABLES_TESTS=ON"
              ]
              ++ cmakeCommonFlags sanitizer;
            sanitizerEnv = { NES_SANITIZER = sanitizer.envValue; };
            envVars = sanitizer.extraEnv // sanitizerEnv;
            sanitizedCompileFlags = lib.concatStringsSep " " sanitizer.compileFlags;
            sanitizedLinkFlags = lib.concatStringsSep " " sanitizer.linkFlags;
          in
          clangStdenv.mkDerivation ((rec {
            pname = "nebulastream";
            version = "unstable";
            src = ./.;

            nativeBuildInputs = buildTools;
            buildInputs = llvmTools ++ cmakeCtx.thirdPartyDeps ++ [ mlirBinary ] ++ sanitizer.extraPackages;
            patches = nebulastreamPatches;
            postPatch = ''
              substituteInPlace CMakeLists.txt \
                --replace "find_package(Protobuf REQUIRED)" "find_package(Protobuf CONFIG REQUIRED)"
            '';
            preConfigure = ''
              export CCACHE_DIR="$TMPDIR/ccache"
              export CCACHE_BASEDIR="$TMPDIR"
              mkdir -p "$CCACHE_DIR"
            '';

            CMAKE_PREFIX_PATH = cmakeCtx.cmakePrefixPath;
            PKG_CONFIG_PATH = cmakeCtx.pkgConfigPath;

            cmakeFlags = cmakeFlagsList;
            NIX_CFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
            NIX_CXXFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
            NIX_LDFLAGS = lib.optionalString (sanitizedLinkFlags != "") sanitizedLinkFlags;

            enableParallelBuilding = true;
            strictDeps = true;
            installPhase = packageInstallPhase;
          }) // envVars);

        mkDevShell = sanitizerName:
          let
            sanitizer = sanitizerOptions.${sanitizerName};
            cmakeCtx = mkCmakeContext sanitizer;
            mlirBinary = mlirForSanitizer sanitizer;
            cmakeFlagsList =
              ccacheFlags
              ++ [ "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON" ]
              ++ cmakeCommonFlags sanitizer;
            sanitizerEnv = { NES_SANITIZER = sanitizer.envValue; };
            envVars =
              sanitizer.extraEnv
              // sanitizerEnv
              // { NES_USE_SYSTEM_DEPS = "ON"; };
            sanitizedCompileFlags = lib.concatStringsSep " " sanitizer.compileFlags;
            sanitizedLinkFlags = lib.concatStringsSep " " sanitizer.linkFlags;
          in
          let
            exportCompileFlagsHook =
              lib.optionalString (sanitizedCompileFlags != "") ''
                export CFLAGS="${sanitizedCompileFlags}''${CFLAGS:+ $CFLAGS}"
                export CXXFLAGS="${sanitizedCompileFlags}''${CXXFLAGS:+ $CXXFLAGS}"
              '';
            exportLinkFlagsHook =
              lib.optionalString (sanitizedLinkFlags != "") ''
                export LDFLAGS="${sanitizedLinkFlags}''${LDFLAGS:+ $LDFLAGS}"
              '';
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
              ''
              + ccacheShellHook
              + exportCompileFlagsHook
              + exportLinkFlagsHook;
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
          antlr4 = antlr4Base;
          cpptrace = cpptraceBase.default;
          nautilus = nautilusPackages.default;
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

            clionRunner = pkgs.writeShellApplication {
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

                # Additionally provide a cmake shim that injects cmakeFlags from Nix
                cmake_src="$project_root/.nix/nix-cmake.sh"
                if [ -x "$cmake_src" ]; then
                  if rel_cmake=$(realpath --relative-to="$bin_dir" "$cmake_src" 2>/dev/null); then
                    cmake_target="$rel_cmake"
                  else
                    cmake_target="$cmake_src"
                  fi
                  ln -sf "$cmake_target" "$bin_dir/cmake"
                  created_list="$created_list\n  - $bin_dir/cmake -> $cmake_target"
                fi

                printf 'clion-setup: shims available in %s\n' "$bin_dir"
                printf '%b\n' "$created_list"
                printf '\nThese binaries re-enter the Nix dev shell on demand; point CLion at them for compilers, ctest, and ninja.\n'
              '';
            };
          in
          {
            clion-setup = {
              type = "app";
              program = "${clionRunner}/bin/clion-setup";
            };
            format = {
              type = "app";
              program = "${formatRunner}/bin/nes-format";
            };
          };

        devShells = devShellVariants // { default = devShellVariants.none; };
      }
    );
}
