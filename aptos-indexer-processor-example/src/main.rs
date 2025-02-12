use anyhow::Result;
use aptos_indexer_processor_example::{
    common::{config::PostgresConfig, processor_name::ProcessorName},
    processors::events::events_processor::EventsProcessor,
};
use aptos_indexer_processor_sdk::server_framework::ServerArgs;
use clap::Parser;
use std::str::FromStr;

#[cfg(unix)]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();

    // TODO: Put the match statement here
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            let args = ServerArgs::parse();
            let processor_name =
                ProcessorName::from_str(args.get_processor_name::<PostgresConfig>()?.as_str())?;
            match processor_name {
                ProcessorName::EventsProcessor => {
                    args.run::<EventsProcessor, PostgresConfig>(
                        EventsProcessor {},
                        tokio::runtime::Handle::current(),
                    )
                    .await
                }
            }
        })
}
