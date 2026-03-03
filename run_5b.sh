cargo build --bin=kafka
../maelstrom/maelstrom test -w kafka --bin ./target/debug/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
