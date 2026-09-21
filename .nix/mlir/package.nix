{
  pkgs,
  llvmToolchainVersion,
  mlirRelease,
  mlirAssetPrefix,
}:
let
  lib = pkgs.lib;
  stdenv = pkgs.stdenvNoCC;
  platform = pkgs.stdenv.hostPlatform;
  arch =
    if platform.isAarch64 then
      "arm64"
    else if platform.isx86_64 then
      "x64"
    else
      builtins.throw "Unsupported system: ${platform.system}";

  supportedSanitizers = [
    "none"
    "address"
    "thread"
    "undefined"
  ];
  supportedStdlibs = [
    "libcxx"
    "libstdcxx"
  ];

  hashes = {
    x64 = {
      none = {
        libcxx = "sha256-Nn86t+YS7YrmLsWPyE1A2vpJi1OmwmLvD/m7UBloO/o=";
        libstdcxx = "sha256-nHalYYu4LEc7vJXojAsCSIm80Uxg1vW886pWFT5cjW8=";
      };
      address = {
        libcxx = "sha256-r+jlVVgo7a5Ite/I6x5JHP5bRPz1jMTa4WInsag+BVY=";
        libstdcxx = "sha256-iVYSpIGFc8/ui2IXlzfU14FKWSkuw+ffkSpyNzBAe5k=";
      };
      thread = {
        libcxx = "sha256-EXQQdi6iqLRifTEPvXBzHrnMuimjJ7zmk2+TN88TgyU=";
        libstdcxx = "sha256-3Yzvmj0SGjVPE5HDU8BRXJ1GvCxCj6xoQ69I3eCmSrk=";
      };
      undefined = {
        libcxx = "sha256-Mt5izF8CpULhYmpmGFNZvYAtpXSrlXSL7fD6HOiYGRM=";
        libstdcxx = "sha256-XwY3CKQB8EPGvxpfaKHgpKAy/r3+EPNm1ZBDJg2ZgVU=";
      };
    };
    arm64 = {
      none = {
        libcxx = "sha256-KiRjfYAO6hgdMaBKou/nEEKStuDMtKxRxbC8J1an7R4=";
        libstdcxx = "sha256-veUbLMx89uRNlDDuRd+5vzmKX52D4r+hLxyUrNX7lvw=";
      };
      address = {
        libcxx = "sha256-zDIkA15NdXkBnq1QzXNRkaJrL3KhG9LFbiFScR770iY=";
        libstdcxx = "sha256-L4/h2X765xDFKISNhbUqgQF+mMdLP3ugHsevscfB/S0=";
      };
      thread = {
        libcxx = "sha256-sGwr4Ek/resX1+RqPl/x5oi6BqQ88ymIDa2nj+Ax13s=";
        libstdcxx = "sha256-Lye3pm1CdmF2WTCmA1gC2jHstk3RcBH+wg+V9CYIyUs=";
      };
      undefined = {
        libcxx = "sha256-Rn9XOuO6Iz4YFx0qMHZwVk67gA/fzXwPAm6n2fBWTDw=";
        libstdcxx = "sha256-3mu38xsng32fN0PXd4aS5Jz2ZgHVX9FC+4176SkY69w=";
      };
    };
  };

  sanitizeSelection =
    list: value:
    if lib.elem value list then value else builtins.throw "Unsupported selection '${value}'";

  hashFor =
    { sanitizer, stdlib }:
    let
      san = sanitizeSelection supportedSanitizers sanitizer;
      std = sanitizeSelection supportedStdlibs stdlib;
      archHashes = hashes.${arch};
    in
    archHashes.${san}.${std};

  mkMlirBinary =
    { sanitizer, stdlib }:
    let
      hash = hashFor { inherit sanitizer stdlib; };
      url = "https://github.com/nebulastream/clang-binaries/releases/download/${mlirRelease}/${mlirAssetPrefix}-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = llvmToolchainVersion;

      src = pkgs.fetchurl {
        inherit url hash;
      };

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

  normalizeArgs =
    args:
    let
      sanitizer = args.sanitizer or "none";
      stdlib = args.stdlib or "libstdcxx";
    in
    {
      inherit sanitizer stdlib;
    };

in
{
  forOptions =
    args:
    let
      cfg = normalizeArgs args;
    in
    mkMlirBinary cfg;

  mlirBinary = mkMlirBinary {
    sanitizer = "none";
    stdlib = "libstdcxx";
  };
}
