fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/influxdb3_replication.proto");
    println!("cargo:rerun-if-changed=proto/influxdb3_distributed_query.proto");

    let replication_proto_path = "proto/influxdb3_replication.proto";
    let distributed_query_proto_path = "proto/influxdb3_distributed_query.proto";
    let proto_dir = "proto";

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Consider different descriptor set names if needed, or a combined one.
        // For now, this might overwrite or be an issue if not handled carefully.
        // Let's use a more generic name or separate them.
        // Using separate descriptor set names:
        .file_descriptor_set_path(format!("{}/replication_descriptor.bin", std::env::var("OUT_DIR").unwrap()))
        .file_descriptor_set_path(format!("{}/distributed_query_descriptor.bin", std::env::var("OUT_DIR").unwrap()))
        .out_dir("src/gen") // Output generated Rust files to src/gen
        .compile(
            &[replication_proto_path, distributed_query_proto_path], // Compile both
            &[proto_dir] // Include directory
        )?;

    Ok(())
}
