# Mini-Kafka Rust Project

## Project Structure

The project is organized as follows:

- `src/`: Contains the main source code for the project.
  - `main.rs`: The entry point of the application. Initiates the broker.
  - `broker/`: Kafka broker implementation.
    - `broker.rs`: Contains constructs and impls for the broker.
    - `decode.rs`: Contains **request** decoding logic as defined by Kafka's wire protocol.
    - `encode.rs`: Contains **response** encoding logic as defined by Kafka's wire protocol. 
    - `process.rs`: Contains the core API logic to process the request from the client.
    - `traits.rs`: Contains broker-specific traits.
    - `utils.rs`: Utility functions.
  - `client/`: Kafka client implementation (`Under development`).
    - `decode.rs`: Contains **response** decoding logic as defined by Kafka's wire protocol.
    - `encode.rs`: Contains **request** encoding logic as defined by Kafka's wire protocol. 
  - `common/`: Contains various modules used in the project.
    - `kafka_protocol.rs`: Contains constructs and impls for Kafka's wire protocol.
    - `kafka_record.rs`: Contains constructs and impls for Kafka's on-disk message storage format.
    - `primitive_types.rs`: Contains the implementations of custom data types as defined by the wire protocol.
  - `errors.rs` - Contains the error types across the project (`Under development`).
- `tests/`: Contains unit and integration tests.
  - `Under development`
- `Cargo.toml`: Contains the project metadata and dependencies.

## Rust Features Used

- **Ownership and Borrowing**: Has has heavy influence on the broker design so far, with respect to how the Kafka's internal components interact (not much impact on the wire protocol itself).
- **Concurrency**: Concurrency primitives such as threads, Arc and Mutex to handle concurrent tasks.
- **Error Handling**: Error handling using `Result` and `Option` types.
- **Traits and Generics**: Uses traits and generics to write reusable code and for dynamic dispatch.
- **Pattern Matching**: Pattern matching for control flow and data manipulation according to data types.

## Getting Started

To build and run the project, use the following commands:
> Note: I used the tester from `codecrafters.io` to aid testing my implementation. My implementation passed all the existing tests. I am working on my own client implementation to test all the core Kafka features as those APIs take shape.

```sh
cargo build
cargo run
```

`[Under Development]` To run the tests, use:

```sh
cargo test
```
