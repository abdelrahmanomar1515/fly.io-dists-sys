cargo build
../maelstrom/maelstrom  test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
