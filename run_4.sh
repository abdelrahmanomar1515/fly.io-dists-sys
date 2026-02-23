cargo build --bin=g_counter
../maelstrom/maelstrom test -w g-counter --bin ./target/debug/g_counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
