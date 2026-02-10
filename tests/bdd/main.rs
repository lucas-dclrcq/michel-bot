#![recursion_limit = "256"]

mod steps;
mod world;

use cucumber::World;

#[tokio::main]
async fn main() {
    world::TestWorld::run("tests/features").await;
    world::stop_shared_infra().await;
}
