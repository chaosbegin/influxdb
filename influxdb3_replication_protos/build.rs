fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // Generate client code
        .compile(
            &["proto/influxdb3_replication.proto"], // Path to your proto file
            &["proto"], // Include directory
        )?;
    Ok(())
}
