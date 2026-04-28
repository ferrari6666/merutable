fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    println!("cargo:rerun-if-changed=proto/raft_transport.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/cluster.proto",
                "proto/raft_transport.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
