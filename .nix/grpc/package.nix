{
  lib,
  llvmPackages_19,
  grpc,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      abseil ? null,
      protobuf ? null,
      re2 ? null,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      overrides = {
        inherit stdenv;
      }
      // lib.optionalAttrs (abseil != null) { abseil-cpp = abseil; }
      // lib.optionalAttrs (protobuf != null) { inherit protobuf; }
      // lib.optionalAttrs (re2 != null) { inherit re2; };
      base = grpc.override overrides;
      concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;
      mergeFlags =
        flags: existing:
        let
          trailing = lib.optional (existing != null && existing != "") existing;
        in
        concatFlags (flags ++ trailing);
    in
    base.overrideAttrs (
      old:
      let
        oldEnv = old.env or { };
      in
      {
        buildInputs = lib.unique ((old.buildInputs or [ ]) ++ extraBuildInputs);
        env = oldEnv // {
          NIX_CFLAGS_COMPILE = mergeFlags compilerFlags (oldEnv.NIX_CFLAGS_COMPILE or "");
        };
      }
    );

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        abseil = null;
        protobuf = null;
        re2 = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
      }
    else if builtins.isAttrs arg then
      {
        extraBuildInputs =
          if arg ? extraBuildInputs then
            arg.extraBuildInputs
          else if arg ? extraPackages then
            arg.extraPackages
          else
            [ ];
        useLibcxx = arg.useLibcxx or false;
        abseil = arg.abseil or null;
        protobuf = arg.protobuf or null;
        re2 = arg.re2 or null;
        compilerFlags = arg.compilerFlags or [ ];
        linkerFlags = arg.linkerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        abseil = null;
        protobuf = null;
        re2 = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build cfg;
}
