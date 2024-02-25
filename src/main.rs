use memmap2::Mmap;
use once_cell::sync::Lazy;
use rustc_hash::{FxHashMap, FxHasher};
use std::{
    collections::BTreeMap,
    fs::File,
    hash::BuildHasherDefault,
    io::Write,
    sync::mpsc::channel,
    thread::{self, available_parallelism},
    time::Instant,
    usize,
};

struct Stats {
    min: i32,
    max: i32,
    sum: i32,
    count: usize,
}

static BUFFER: Lazy<Mmap> = Lazy::new(|| {
    let file = File::open("measurements.txt").unwrap();
    unsafe { Mmap::map(&file).unwrap() }
});

fn main() {
    let num_threads = 10 * available_parallelism().unwrap().get();
    let (tx, rx) = channel();
    let chunks = chunks(&BUFFER, num_threads);
    let num_chunks = chunks.len();

    let time = Instant::now();
    for chunk in chunks {
        let tx = tx.clone();
        thread::spawn(move || {
            let mut cities_stats: FxHashMap<&[u8], Stats> =
                FxHashMap::with_capacity_and_hasher(100, BuildHasherDefault::<FxHasher>::default());
            let mut i = 0;
            while i < chunk.len() {
                let (city, measure, last) = parse_next_row(&chunk[i..]);
                let stats = cities_stats.entry(city).or_insert(Stats {
                    min: i32::MAX,
                    max: i32::MIN,
                    sum: 0,
                    count: 0,
                });
                stats.min = measure.min(stats.min);
                stats.max = measure.max(stats.max);
                stats.count += 1;
                stats.sum += measure;
                i += last;
            }
            tx.send(cities_stats).unwrap();
        });
    }

    let mut i = 0;
    let mut cities_stats: BTreeMap<&[u8], Stats> = BTreeMap::new();
    while i < num_chunks {
        if let Ok(work) = rx.recv() {
            for (city, stats) in work {
                if cities_stats.contains_key(city) {
                    let global_stats = cities_stats.get_mut(city).unwrap();
                    global_stats.min = stats.min.min(global_stats.min);
                    global_stats.max = stats.max.max(global_stats.max);
                    global_stats.sum += stats.sum;
                    global_stats.count += stats.count;
                } else {
                    cities_stats.insert(city, stats);
                }
            }
            i += 1;
        }
    }

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    write!(lock, "{{").unwrap();
    let mut c = 0;
    for (city, stats) in &cities_stats {
        write!(
            lock,
            "{}={}/{:.2}/{}",
            std::str::from_utf8(city).unwrap(),
            stats.min as f32 / 10.0,
            stats.sum as f32 / stats.count as f32 / 10.0,
            stats.max as f32 / 10.0
        )
        .unwrap();
        c += 1;
        if c != cities_stats.len() {
            write!(lock, ", ").unwrap();
        }
    }
    write!(lock, "}}").unwrap();
    writeln!(lock, "{:?}", time.elapsed()).unwrap();
}

#[inline(always)]
fn chunks(buffer: &[u8], num_threads: usize) -> Vec<&[u8]> {
    let mut result = vec![];
    let chunk_size = buffer.len() / num_threads;
    let mut i = 0;
    while i <= buffer.len() {
        let s = i;
        i = if i + chunk_size < buffer.len() {
            i + chunk_size
        } else {
            buffer.len()
        };
        while i < buffer.len() && buffer[i] != b'\n' {
            i += 1;
        }
        result.push(&buffer[s..i]);
        i += 1;
    }

    result
}

#[inline(always)]
fn parse_next_row(slice: &[u8]) -> (&[u8], i32, usize) {
    let mut i = 0;
    while slice[i] != b';' {
        i += 1;
    }
    let end_city = i;
    i += 1;
    let sign: i32 = if slice[i] == b'-' {
        i += 1;
        -1
    } else {
        1
    };
    let mut measure = sign * (slice[i] - b'0') as i32;
    i += 1;
    if slice[i] != b'.' {
        measure = measure * 10 + (slice[i] - b'0') as i32;
        i += 1;
    }
    i += 1;
    measure = 10 * measure + (slice[i] - b'0') as i32;
    i += 1;

    return (&slice[0..end_city], measure, i + 1);
}

#[cfg(test)]
mod test {
    use crate::{chunks, parse_next_row};
    use pretty_assertions::assert_eq;

    fn content() -> &'static [u8] {
        r#"Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0"#
            .as_bytes()
    }

    #[test]
    fn it_chunks_content() {
        let content = content();
        assert_eq!(
            vec![
                r#"Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2"#
                    .as_bytes(),
                r#"Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4"#
                    .as_bytes(),
                r#"Conakry;31.2
Istanbul;23.0"#
                    .as_bytes()
            ],
            chunks(content, 3)
        );
    }

    #[test]
    fn it_parses_row() {
        let content = content();

        let mut result: Vec<(&[u8], i32)> = vec![];
        let mut i = 0;
        while i < content.len() {
            let (city, measure, last) = parse_next_row(&content[i..]);
            result.push((city, measure));
            i += last;
        }

        assert_eq!(
            vec![
                ("Hamburg".as_bytes(), 120),
                ("Bulawayo".as_bytes(), 89),
                ("Palembang".as_bytes(), 388),
                ("St. John's".as_bytes(), 152),
                ("Cracow".as_bytes(), 126),
                ("Bridgetown".as_bytes(), 269),
                ("Istanbul".as_bytes(), 62),
                ("Roseau".as_bytes(), 344),
                ("Conakry".as_bytes(), 312),
                ("Istanbul".as_bytes(), 230),
            ],
            result
        );
    }
}
