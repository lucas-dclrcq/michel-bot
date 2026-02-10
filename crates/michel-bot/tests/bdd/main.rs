#![recursion_limit = "256"]

mod steps;
mod world;

use crate::world::TestWorld;
use cucumber::{World, writer};

#[tokio::main]
async fn main() {
    TestWorld::cucumber()
        .with_writer(writer::Libtest::or_basic())
        .run("tests/features")
        .await;

    world::stop_shared_infra().await;
}
