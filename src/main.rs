use rusqlite::Connection;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Instant;
use clap::Parser;


const PHONE_TITLES: [&str; 33] = [
    "139", "138", "137", "136", "135", "134", "159", "158", "157", "150", "151", "152", "188",
    "187", "182", "183", "184", "178", "130", "131", "132", "156", "155", "186", "185", "176",
    "133", "153", "189", "180", "181", "177", "199",
];

fn get_random_phone() -> String {
    let idx = fastrand::usize(..PHONE_TITLES.len());
    let mut phone = PHONE_TITLES[idx].to_string();
    phone.push_str(&fastrand::i64(10000000..99999999).to_string());
    return phone;
}

fn consumer(rx: Receiver<Vec<String>>, dbpath: String) {
    let mut conn = Connection::open(dbpath).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
    )
    .expect("PRAGMA");

    conn.execute(
        "create table if not exists phone (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            value    CHAR(11))",
        [],
    )
    .unwrap();
    let tx = conn.transaction().unwrap();
    {
        let sql = "INSERT INTO phone(value) VALUES (?)";
        let mut stmt = tx.prepare_cached(sql).unwrap();
        for values in rx {
            for value in values {
                stmt.execute(&[&value]).unwrap();
            }
        }
    }
    tx.commit().unwrap();
}

fn producer(tx: Sender<Vec<String>>, count: i64) {
    let mut values = Vec::<String>::new();
    for _ in 0..count {
        let phone = get_random_phone();
        values.push(phone)
    }
    tx.send(values).unwrap();
}

/// 随机生成手机号并写入SQLite
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 输出文件路径(SQLite文件), example: --out ./output.db
    #[arg(short, long, default_value_t = String::from("output.db"))]
    out: String,

    /// 生成手机号的数量, example: --num 1000000
    #[arg(short, long, default_value_t = 1000)]
    num: usize,
}


fn main() {
    let args = Args::parse();
    let start = Instant::now();
    let (tx, rx): (Sender<Vec<String>>, Receiver<Vec<String>>) = mpsc::channel();
    let consumer_handle = thread::spawn(|| consumer(rx, args.out));
    let cpu_count = num_cpus::get();
    let remaining = (args.num % cpu_count) as i64;
    let each_producer_count = (args.num / cpu_count) as i64;
    
    let mut handles: Vec<_> = Vec::with_capacity(cpu_count);
    for i in 0..cpu_count {
        let thread_tx = tx.clone();
        if i == 0 {
            handles.push(thread::spawn(move || producer(thread_tx, each_producer_count + remaining)))
        } else {
            handles.push(thread::spawn(move || producer(thread_tx, each_producer_count)))
        }
    }
    for handler in handles {
        handler.join().unwrap();
    }
    drop(tx);
    // wait till consumer is exited
    consumer_handle.join().unwrap();

    println!(
        "success, total count {} in {:.2}s",
        each_producer_count * cpu_count as i64,
        start.elapsed().as_secs_f32()
    );
}
