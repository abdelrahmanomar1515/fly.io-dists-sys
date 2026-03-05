cargo build --bin=txn
../maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
