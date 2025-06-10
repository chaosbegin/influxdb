fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure tonic_build to compile the replication service protobuf.
    // It's assumed that the .proto file is located at "proto/influxdb3_replication.proto"
    // relative to the crate root (influxdb3_server).
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // Enable client generation for DistributedQueryService
        .compile(
            &[
                "proto/influxdb3_replication.proto",
                "proto/influxdb3_distributed_query.proto" // Added new proto
            ],
            &["proto/"], // Include directory for imports
        )?;
    Ok(())
}
