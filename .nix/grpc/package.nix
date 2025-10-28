{ pkgs, abseilCpp ? pkgs.abseil-cpp_202401, protobufPkg ? pkgs.protobuf }:

let
  lib = pkgs.lib;
  sanitizerSupport = import ../lib/sanitizer-support.nix { inherit lib; };
  mkSanitizedPackage = sanitizerSupport.mkSanitizedPackage;

  # No source patches by default; configure via flags below to avoid patch failures
  repoPatches = [ ];


  build = {
    extraBuildInputs ? [ ],
    sanitizeCompileFlags ? [ ],
    sanitizeLinkFlags ? [ ]
  }:
    let
      baseDrv = (pkgs.grpc.override { "abseil-cpp" = abseilCpp; protobuf = protobufPkg; }).overrideAttrs (old: let
        # Remove any abseil-cpp from buildInputs/propagated... and add our abseilCpp
        rmAbsl = pkgsList:
          builtins.filter (p: let n = (p.pname or (p.name or "")); in n != "abseil-cpp") pkgsList;
        buildInputs' = (rmAbsl (old.buildInputs or [ ])) ++ [ abseilCpp.dev or abseilCpp ];
        propagatedBuildInputs' = (rmAbsl (old.propagatedBuildInputs or [ ])) ++ [ abseilCpp ];
        cmakeFlags' = (old.cmakeFlags or [ ]) ++ [
          "-DgRPC_BUILD_CODEGEN=OFF"
          "-DgRPC_BUILD_TESTS=OFF"
          # Proactively disable all plugin targets that depend on protobuf compiler headers
          "-DgRPC_BUILD_GRPC_CPP_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF"
          "-DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF"
          # Some builds gate the common support lib behind this toggle
          "-DgRPC_BUILD_PLUGIN_SUPPORT=OFF"
          "-DCMAKE_CXX_FLAGS=-DABSL_INTERNAL_ENABLE_TYPE_ERASURE_CONSTEXPR_TESTING=0"
          "-DCMAKE_C_FLAGS=-DABSL_INTERNAL_ENABLE_TYPE_ERASURE_CONSTEXPR_TESTING=0"
        ];
      in {
        patches = (old.patches or [ ]) ++ repoPatches;
        cmakeFlags = cmakeFlags';
        buildInputs = buildInputs';
        propagatedBuildInputs = propagatedBuildInputs';
        postPatch = (old.postPatch or "") + ''
          # Transform grpc_plugin_support into an INTERFACE target to avoid compiling generator sources
          awk '
          BEGIN{ skipping=0 }
          {
            if (skipping==1) {
              if ($0 ~ /\)/) { skipping=0; next }
              else { next }
            }
            if ($0 ~ /^[[:space:]]*add_library[[:space:]]*\(grpc_plugin_support\b/) {
              print "add_library(grpc_plugin_support INTERFACE)"
              skipping=1
              next
            }
            print
          }
          ' CMakeLists.txt > CMakeLists.txt.tmp && mv CMakeLists.txt.tmp CMakeLists.txt
        '';
      });
    in
    sanitizerSupport.sanitizePackage {
      sanitizer = {
        compileFlags = sanitizeCompileFlags;
        linkFlags = sanitizeLinkFlags;
      };
      drv = baseDrv;
      extraBuildInputs = extraBuildInputs;
    };

in mkSanitizedPackage build
