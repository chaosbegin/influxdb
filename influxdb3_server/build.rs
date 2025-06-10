fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure tonic_build to compile the replication service protobuf.
    // It's assumed that the .proto file is located at "proto/influxdb3_replication.proto"
    // relative to the crate root (influxdb3_server).
    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(false) // Client might be in a different crate or not needed here
        .compile(
            &["proto/influxdb3_replication.proto"], // Path to .proto file
            &["proto/"], // Include directory for imports if any
        )?;
    Ok(())
}
