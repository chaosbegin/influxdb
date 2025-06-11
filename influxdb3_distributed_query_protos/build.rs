fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get the path to the well-known protobuf files from the `prost-types` crate
    let prost_types_include = prost_types::include_path();

    // Attempt to find arrow_flight.proto via arrow-flight crate's features or expect it to be discoverable
    // This part is tricky and might require a more robust solution if arrow-flight crate doesn't expose its protos easily for tonic-build.
    // A common pattern is to vendor required .proto files or use features of crates like arrow-flight if they provide them.
    // For now, we hope that including prost_types and potentially having arrow-flight as a (dev-)dependency
    // might make arrow_flight.proto discoverable or that its types are correctly mapped via extern_path if needed.
    //
    // A more robust way if direct compilation fails:
    // 1. Vendor arrow_flight.proto into this crate's proto directory.
    // 2. Or, use `tonic_build::configure().extern_path(".arrow.flight.protocol.FlightData", "::arrow_flight::FlightData")`
    //    if we intend to use Rust types from the arrow-flight crate directly for FlightData fields.
    //    However, the proto defines the service returning `stream arrow.flight.protocol.FlightData`,
    //    so tonic-build needs to understand this type.

    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // Generate client code
        .compile_well_known_types(true) // For google.protobuf types, might help with some transitive dependencies
        .extern_path(".google.protobuf.Any", "::prost_types::Any") // Example for well-known types
        .compile(
            &["proto/influxdb3_distributed_query.proto"], // Path to your proto file
            &["proto", &prost_types_include], // Include directory, and prost_types for Any etc.
        )?;
    Ok(())
}
