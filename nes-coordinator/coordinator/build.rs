fn main() {
    println!("cargo::rustc-check-cfg=cfg(madsim)");

    tonic_build::configure()
        .compile_protos(
            &["../../grpc/SingleNodeWorkerRPCService.proto"],
            &["../../grpc"],
        )
        .unwrap();
}
