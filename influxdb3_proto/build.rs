fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/influxdb3_replication.proto");

    let proto_path = "proto/influxdb3_replication.proto";
    let proto_dir = "proto";

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(std::env::var("OUT_DIR").unwrap() + "/replication_descriptor.bin") // Example for descriptor set
        .out_dir("src/gen") // Output generated Rust files to src/gen
        .compile(&[proto_path], &[proto_dir])?;

    Ok(())
}
