fn main() {
    println!("cargo::rustc-check-cfg=cfg(madsim)");

    tonic_build::configure()
        .compile_protos(
            &[
                "../proto/SingleNodeWorkerRPCService.proto",
                "../proto/health.proto",
            ],
            &["../proto"],
        )
        .unwrap();
}
