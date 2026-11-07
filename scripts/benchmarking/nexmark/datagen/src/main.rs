// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writes a deterministic Nexmark event stream as three headerless CSV files
//! (person.csv, auction.csv, bid.csv) with the full Flink Nexmark schema, plus the
//! q13 side input (side_input.csv).
//! The same files feed NebulaStream and the Flink reference run.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use nexmark::config::NexmarkConfig;
use nexmark::event::Event;
use nexmark::EventGenerator;

#[derive(Debug, Parser)]
struct Args {
    /// Total number of events (persons + auctions + bids).
    #[clap(short, long)]
    events: usize,

    /// Output directory.
    #[clap(short, long)]
    out: PathBuf,

    /// Event-time origin in epoch milliseconds. Fixed so that runs are reproducible.
    #[clap(long, default_value_t = 1_700_000_000_000)]
    base_time: u64,

    /// Events per second of event time.
    #[clap(long, default_value_t = 10_000)]
    rate: usize,

    /// Rows of the q13 side input (`key,value` = `i,i`, as upstream's SideInputGenerator writes them).
    #[clap(long, default_value_t = 10_000)]
    side_input_rows: usize,
}

/// The CSV readers on both sides do not quote, so a field must not contain a separator.
fn field(value: &str) -> &str {
    assert!(
        !value.contains([',', '\n', '\r', '"']),
        "generated string needs quoting: {value:?}"
    );
    value
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;

    let mut config = NexmarkConfig::default();
    config.base_time = args.base_time;
    config.first_rate = args.rate;
    config.next_rate = args.rate;
    // nexmark-rs lowercases the states; the Java generator used by Flink emits them uppercase
    // and q3 filters on 'OR', 'ID', 'CA'.
    config.us_states = ["AZ", "CA", "ID", "OR", "WA", "WY"].map(String::from).to_vec();

    let open = |name: &str| File::create(args.out.join(name)).map(|f| BufWriter::with_capacity(1 << 20, f));
    let mut person = open("person.csv")?;
    let mut auction = open("auction.csv")?;
    let mut bid = open("bid.csv")?;

    for event in EventGenerator::new(config).take(args.events) {
        match event {
            Event::Person(p) => writeln!(
                person,
                "{},{},{},{},{},{},{},{}",
                p.id,
                field(&p.name),
                field(&p.email_address),
                field(&p.credit_card),
                field(&p.city),
                field(&p.state),
                p.date_time,
                field(&p.extra)
            )?,
            Event::Auction(a) => writeln!(
                auction,
                "{},{},{},{},{},{},{},{},{},{}",
                a.id,
                field(&a.item_name),
                field(&a.description),
                a.initial_bid,
                a.reserve,
                a.date_time,
                a.expires,
                a.seller,
                a.category,
                field(&a.extra)
            )?,
            Event::Bid(b) => writeln!(
                bid,
                "{},{},{},{},{},{},{}",
                b.auction,
                b.bidder,
                b.price,
                field(&b.channel),
                field(&b.url),
                b.date_time,
                field(&b.extra)
            )?,
        }
    }
    person.flush()?;
    auction.flush()?;
    bid.flush()?;

    let mut side_input = open("side_input.csv")?;
    for i in 0..args.side_input_rows {
        writeln!(side_input, "{i},{i}")?;
    }
    side_input.flush()
}
