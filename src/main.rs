use memmap2::Mmap;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    sync::mpsc::channel,
    thread::{self, available_parallelism},
    usize,
};

struct Stats {
    min: f32,
    max: f32,
    avg: f32,
    count: usize,
}

static BUFFER: Lazy<Mmap> = Lazy::new(|| {
    let file = File::open("measurements.txt").unwrap();
    unsafe { Mmap::map(&file).unwrap() }
});

fn main() {
    let num_threads = available_parallelism().unwrap().get();
    let (tx, rx) = channel();
    let chunks = chunks(&BUFFER, num_threads);
    let num_chunks = chunks.len();

    for chunk in chunks {
        let tx = tx.clone();
        thread::spawn(move || {
            let mut cities_stats: FxHashMap<&[u8], Stats> = FxHashMap::default();
            let mut i = 0;
            while i < chunk.len() {
                let (city, measure, last) = parse_next_row(&chunk[i..]);
                let stats = cities_stats.entry(city).or_insert(Stats {
                    min: f32::MAX,
                    max: f32::MIN,
                    avg: 0.0,
                    count: 0,
                });
                stats.min = measure.min(stats.min);
                stats.max = measure.max(stats.max);
                stats.avg = ((stats.avg * stats.count as f32) + measure) / (stats.count + 1) as f32;
                stats.count += 1;
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
                    global_stats.avg = (stats.avg + global_stats.avg) / 2.0;
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
            "{}={}/{}/{}",
            std::str::from_utf8(city).unwrap(),
            stats.min,
            stats.avg,
            stats.max
        )
        .unwrap();
        c += 1;
        if c != cities_stats.len() {
            write!(lock, ", ").unwrap();
        }
    }
    write!(lock, "}}").unwrap();
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
fn parse_next_row(slice: &[u8]) -> (&[u8], f32, usize) {
    let mut i = 0;
    while slice[i] != b';' {
        i += 1;
    }
    let end_city = i;
    while i < slice.len() && slice[i] != b'\n' {
        i += 1;
    }
    return (
        &slice[0..end_city],
        std::str::from_utf8(&slice[end_city + 1..i])
            .unwrap()
            .parse()
            .unwrap(),
        i + 1,
    );
}

#[cfg(test)]
mod test {
    use crate::{chunks, parse_next_row};

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

        let mut result: Vec<(&[u8], f32)> = vec![];
        let mut i = 0;
        while i < content.len() {
            let (city, measure, last) = parse_next_row(&content[i..]);
            result.push((city, measure));
            i += last;
        }

        assert_eq!(
            vec![
                ("Hamburg".as_bytes(), 12.0f32),
                ("Bulawayo".as_bytes(), 8.9f32),
                ("Palembang".as_bytes(), 38.8f32),
                ("St. John's".as_bytes(), 15.2f32),
                ("Cracow".as_bytes(), 12.6f32),
                ("Bridgetown".as_bytes(), 26.9f32),
                ("Istanbul".as_bytes(), 6.2f32),
                ("Roseau".as_bytes(), 34.4f32),
                ("Conakry".as_bytes(), 31.2f32),
                ("Istanbul".as_bytes(), 23.0f32),
            ],
            result
        );
    }
}
