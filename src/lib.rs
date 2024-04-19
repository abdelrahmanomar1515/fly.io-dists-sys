use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io::Write,
    sync::mpsc,
    thread::{self, sleep},
    time::Duration,
};

pub trait Handle {
    fn handle(&mut self) -> anyhow::Result<()>;
}

pub fn main_loop<NodeCreator, Node, Message>(make_node: NodeCreator) -> anyhow::Result<()>
where
    NodeCreator: FnOnce(mpsc::Receiver<()>, mpsc::Receiver<Message>, mpsc::Sender<Message>) -> Node,
    Node: Handle,
    Message: Serialize
        + for<'a> Deserialize<'a>
        + Debug
        + std::marker::Send
        + std::marker::Sync
        + 'static,
{
    let (stdout_send, stdout_recv) = mpsc::channel();
    let (msg_send, msg_recv) = mpsc::channel();
    let (timer_send, timer_recv) = mpsc::channel::<()>();

    thread::spawn(move || -> anyhow::Result<()> {
        let mut stdout = std::io::stdout().lock();
        loop {
            if let Ok(msg) = stdout_recv.recv() {
                eprintln!("output: {:?}", msg);
                serde_json::to_writer(&mut stdout, &msg).context("Serialize to stdout")?;
                writeln!(stdout).context("write to stdout")?
            }
        }
    });

    thread::spawn(move || -> anyhow::Result<()> {
        loop {
            sleep(Duration::from_millis(100));
            timer_send.send(())?;
        }
    });

    thread::spawn(move || -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for input in inputs {
            let input = input.context("deserialize stdin")?;
            eprintln!("inputs: {:?}", input);
            msg_send.send(input)?;
        }

        eprintln!("input stream finished");
        Ok(())
    });

    let mut node = make_node(timer_recv, msg_recv, stdout_send);
    node.handle().context("handling failed")?;

    eprintln!("somehow finished");

    Ok(())
}
