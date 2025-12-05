#![feature(portable_simd)]
#![feature(hasher_prefixfree_extras)]
use memmap2::Mmap;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    hash::{BuildHasher, Hasher},
    io::Write,
    simd::{cmp::SimdPartialEq, num::SimdUint, u8x64, u8x8},
    sync::mpsc::channel,
    thread::available_parallelism,
};

const SEMI: u8x8 = u8x8::splat(b';');
const NEWL: u8x64 = u8x64::splat(b'\n');

struct Stats {
    min: i16,
    max: i16,
    sum: i32,
    count: u32,
}

struct FastHasherBuilder;
struct FastHasher(u64);

impl BuildHasher for FastHasherBuilder {
    type Hasher = FastHasher;

    fn build_hasher(&self) -> Self::Hasher {
        FastHasher(0xcbf29ce484222325)
    }
}

impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0 ^ self.0.rotate_right(33) ^ self.0.rotate_right(15)
    }

    fn write_length_prefix(&mut self, _len: usize) {}

    fn write(&mut self, bytes: &[u8]) {
        let mut word = [0u64; 2];
        unsafe {
            std::ptr::copy(
                bytes.as_ptr(),
                word.as_mut_ptr().cast::<u8>(),
                bytes.len().min(16),
            )
        };
        self.0 = word[0] ^ word[1];
    }
}

fn main() {
    let mut cities_stats: BTreeMap<&[u8], Stats> = BTreeMap::new();
    let map = unsafe { Mmap::map(&File::open("measurements.txt").unwrap()).unwrap() };
    std::thread::scope(|scope| {
        let num_threads = available_parallelism().unwrap().get();
        let (tx, rx) = channel();
        let chunks = chunks(&map, map.len() / num_threads);

        for chunk in chunks {
            let tx = tx.clone();
            scope.spawn(move || {
                let mut cities_stats = HashMap::with_capacity_and_hasher(1_024, FastHasherBuilder);
                let mut i = 0;
                while i < chunk.len() {
                    let (city, measure, last) = parse_next_row(&chunk[i..]);
                    let stats = cities_stats.entry(city).or_insert(Stats {
                        min: i16::MAX,
                        max: i16::MIN,
                        sum: 0,
                        count: 0,
                    });
                    stats.min = measure.min(stats.min);
                    stats.max = measure.max(stats.max);
                    stats.count += 1;
                    stats.sum += measure as i32;
                    i += last;
                }
                tx.send(cities_stats).unwrap();
            });
        }
        drop(tx);

        for one_stat in rx {
            for (city, stats) in one_stat {
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
        }
    });

    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    write!(lock, "{{").unwrap();
    let mut c = 0;
    for (city, stats) in &cities_stats {
        write!(
            lock,
            "{}={}/{:.2}/{}",
            unsafe { std::str::from_utf8_unchecked(city) },
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
}

#[inline(always)]
fn chunks(buffer: &[u8], chunk_size: usize) -> Vec<&[u8]> {
    let mut result = vec![];
    let mut i = 0;
    while i <= buffer.len() {
        let s = i;
        i = (i + chunk_size).min(buffer.len());
        i += next_newline(&buffer[i..]);
        result.push(&buffer[s..i]);
        i += 1;
    }

    result
}

#[inline(always)]
fn next_newline_part(chunk: &[u8]) -> (bool, u8) {
    let chunk = u8x64::load_or_default(chunk);

    let bm = chunk.simd_eq(NEWL).to_bitmask();
    let pos = bm.trailing_zeros() as u8;
    let found = bm != 0;

    (found, pos)
}

#[inline(always)]
fn next_newline(remaning: &[u8]) -> usize {
    let (found1, pos1) = next_newline_part(remaning);
    let (found2, pos2) = next_newline_part(&remaning[64.min(remaning.len())..remaning.len()]);

    ((found1 as u8 * pos1)
        | ((!found1) as u8 * found2 as u8 * pos2)
        | ((!found1) as u8 * (!found2) as u8 * remaning.len() as u8)) as usize
}

#[inline(always)]
fn parse_next_row(remaning: &[u8]) -> (&[u8], i16, usize) {
    let end_line = next_newline(remaning);
    let line = &remaning[..end_line];

    let measure_bytes = u8x8::load_or_default(&line[end_line - 6..]);

    let measure_start_pos = unsafe { measure_bytes.simd_eq(SEMI).first_set().unwrap_unchecked() };
    let row_delimiter_pos = line.len() - (6 - measure_start_pos);

    let digits_mask = u8x8::splat(b'0');
    let measure_parts = measure_bytes - digits_mask;

    let sign = -((measure_parts[measure_start_pos + 1] > 9) as i16);

    let hundreds = (measure_parts[2] < 9) as i16 * measure_parts[2] as i16 * 100i16;

    let significand = u8x8::from_array([0, 0, 0, 10, 0, 1, 0, 0]) * measure_parts;
    let significand = significand.reduce_sum() as i16 + hundreds;

    let measure = (significand ^ sign) - sign;
    (&line[0..row_delimiter_pos], measure, end_line + 1)
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
Roseau;-34.4
Conakry;31.2
Istanbul;23.0"#
            .as_bytes()
    }

    #[test]
    fn it_chunks_content() {
        let content = content();
        let res = chunks(content, 48);

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
Roseau;-34.4"#
                    .as_bytes(),
                r#"Conakry;31.2
Istanbul;23.0"#
                    .as_bytes()
            ],
            res
        );
    }

    #[test]
    fn it_parses_rows() {
        let content = content();

        let mut result: Vec<(&[u8], i16)> = vec![];
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
                ("Roseau".as_bytes(), -344),
                ("Conakry".as_bytes(), 312),
                ("Istanbul".as_bytes(), 230),
            ],
            result
        );
    }
}
