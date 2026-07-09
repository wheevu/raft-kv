use crate::state_machine::StateMachine;
use crate::{Command, LogIndex};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"RKVSST01";
const INDEX_STRIDE: usize = 16;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 7;

#[derive(Clone, Copy, Debug)]
pub struct LsmOptions {
    pub memtable_bytes: usize,
    pub compaction_threshold: usize,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_bytes: 1024 * 1024,
            compaction_threshold: 4,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Record {
    key: String,
    value: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WalFrame {
    index: LogIndex,
    record: Option<Record>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DataEntry {
    index: LogIndex,
    record: Record,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SstableFooter {
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    max_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IndexEntry {
    key: String,
    offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BloomFilter {
    bits: Vec<u8>,
    bit_len: usize,
}

impl BloomFilter {
    fn new(keys: impl Iterator<Item = String>, count: usize) -> Self {
        let bit_len = (count.max(1) * BLOOM_BITS_PER_KEY).max(8);
        let mut filter = Self {
            bits: vec![0; bit_len.div_ceil(8)],
            bit_len,
        };
        for key in keys {
            filter.insert(&key);
        }
        filter
    }

    fn insert(&mut self, key: &str) {
        let bits: Vec<_> = self.bits_for(key).collect();
        for bit in bits {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    fn might_contain(&self, key: &str) -> bool {
        self.bits_for(key)
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn bits_for<'a>(&'a self, key: &'a str) -> impl Iterator<Item = usize> + 'a {
        let h1 = hash_with_seed(key.as_bytes(), 0xcbf2_9ce4_8422_2325);
        let h2 = hash_with_seed(key.as_bytes(), 0x9e37_79b9_7f4a_7c15) | 1;
        (0..BLOOM_HASHES).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) as usize % self.bit_len)
    }
}

#[derive(Debug)]
struct Sstable {
    id: u64,
    path: PathBuf,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    max_index: LogIndex,
}

#[derive(Debug)]
pub struct LsmTree {
    dir: PathBuf,
    wal_path: PathBuf,
    wal: File,
    memtable: BTreeMap<String, (Option<String>, LogIndex)>,
    memtable_bytes: usize,
    tables: Vec<Sstable>,
    options: LsmOptions,
    last_applied: LogIndex,
    disk_reads: usize,
}

impl LsmTree {
    pub fn open(dir: impl AsRef<Path>, options: LsmOptions) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let wal_path = dir.join("wal.log");
        let mut tables = load_tables(&dir)?;
        tables.sort_by_key(|table| std::cmp::Reverse(table.id));
        let mut last_applied = tables.iter().map(|table| table.max_index).max().unwrap_or(0);
        let mut memtable = BTreeMap::new();
        let mut memtable_bytes = 0;
        if wal_path.exists() {
            for frame in read_wal_frames_truncating(&wal_path)? {
                if frame.index <= last_applied {
                    continue;
                }
                if let Some(record) = frame.record {
                    memtable_bytes += record_size(&record);
                    memtable.insert(record.key.clone(), (record.value, frame.index));
                }
                last_applied = frame.index;
            }
        }
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        Ok(Self {
            dir,
            wal_path,
            wal,
            memtable,
            memtable_bytes,
            tables,
            options,
            last_applied,
            disk_reads: 0,
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let id = self.next_table_id();
        let entries: Vec<_> = self
            .memtable
            .iter()
            .map(|(key, (value, index))| DataEntry {
                index: *index,
                record: Record {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect();
        let table = write_table(&self.dir, id, &entries)?;
        self.tables.insert(0, table);
        self.memtable.clear();
        self.memtable_bytes = 0;
        self.reset_wal()?;
        self.maybe_compact()?;
        Ok(())
    }

    pub fn compact(&mut self) -> io::Result<()> {
        if self.tables.len() < 2 {
            return Ok(());
        }
        let old_tables = std::mem::take(&mut self.tables);
        let mut merged: BTreeMap<String, DataEntry> = BTreeMap::new();
        for table in &old_tables {
            for entry in read_all_entries(&table.path)? {
                merged.entry(entry.record.key.clone()).or_insert(entry);
            }
        }
        let entries: Vec<_> = merged
            .into_values()
            .filter(|entry| entry.record.value.is_some())
            .collect();
        let new_table = if entries.is_empty() {
            None
        } else {
            Some(write_table(&self.dir, self.next_table_id_from(&old_tables), &entries)?)
        };
        if let Some(table) = new_table {
            self.tables.push(table);
        }
        // Deleting source tables happens only after the replacement table is durable.
        for table in old_tables {
            let _ = fs::remove_file(table.path);
        }
        self.tables.sort_by_key(|table| std::cmp::Reverse(table.id));
        sync_dir(&self.dir);
        Ok(())
    }

    pub fn sstable_count(&self) -> usize {
        self.tables.len()
    }

    pub fn disk_read_count(&self) -> usize {
        self.disk_reads
    }

    #[cfg(test)]
    pub(crate) fn append_wal_only_for_test(
        &mut self,
        index: LogIndex,
        command: &Command,
    ) -> io::Result<()> {
        let record = record_from_command(command);
        self.append_wal(WalFrame { index, record })
    }

    fn apply_record(&mut self, index: LogIndex, record: Option<Record>) -> io::Result<()> {
        if index <= self.last_applied {
            return Ok(());
        }
        if index != self.last_applied + 1 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "state machine apply index gap"));
        }
        // The Raft log index is part of the storage commit record. This lets recovery
        // distinguish "already applied" from "must replay" without relying on commands
        // being idempotent.
        self.append_wal(WalFrame { index, record: record.clone() })?;
        if let Some(record) = record {
            self.memtable_bytes += record_size(&record);
            self.memtable.insert(record.key, (record.value, index));
        }
        self.last_applied = index;
        if self.memtable_bytes >= self.options.memtable_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn append_wal(&mut self, frame: WalFrame) -> io::Result<()> {
        write_frame(&mut self.wal, &frame)?;
        self.wal.sync_all()
    }

    fn get_record(&mut self, key: &str) -> io::Result<Option<Option<String>>> {
        if let Some((value, _)) = self.memtable.get(key) {
            return Ok(Some(value.clone()));
        }
        for idx in 0..self.tables.len() {
            if !self.tables[idx].bloom.might_contain(key) {
                continue;
            }
            self.disk_reads += 1;
            if let Some(value) = lookup_table(&self.tables[idx], key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.tables.len() >= self.options.compaction_threshold {
            self.compact()?;
        }
        Ok(())
    }

    fn next_table_id(&self) -> u64 {
        self.tables.iter().map(|table| table.id).max().unwrap_or(0) + 1
    }

    fn next_table_id_from(&self, tables: &[Sstable]) -> u64 {
        tables.iter().map(|table| table.id).max().unwrap_or(0).max(self.next_table_id()) + 1
    }

    fn reset_wal(&mut self) -> io::Result<()> {
        self.wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&self.wal_path)?;
        self.wal.sync_all()
    }
}

impl StateMachine for LsmTree {
    fn apply(&mut self, index: LogIndex, command: &Command) -> io::Result<()> {
        self.apply_record(index, record_from_command(command))
    }

    fn get(&self, key: &str) -> io::Result<Option<String>> {
        // StateMachine::get takes &self for in-memory implementations. LSM point reads
        // need a disk-read counter for tests, so public LSM tests call get_mut directly.
        let mut clone = self.read_only_clone()?;
        clone.get_mut(key)
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

impl LsmTree {
    pub fn get_mut(&mut self, key: &str) -> io::Result<Option<String>> {
        Ok(self.get_record(key)?.flatten())
    }

    fn read_only_clone(&self) -> io::Result<Self> {
        Self::open(&self.dir, self.options)
    }
}

fn record_from_command(command: &Command) -> Option<Record> {
    match command {
        Command::Noop => None,
        Command::Set { key, value } => Some(Record { key: key.clone(), value: Some(value.clone()) }),
        Command::Delete { key } => Some(Record { key: key.clone(), value: None }),
    }
}

fn write_table(dir: &Path, id: u64, entries: &[DataEntry]) -> io::Result<Sstable> {
    let path = dir.join(format!("sst-{id:020}.sst"));
    let tmp = dir.join(format!(".sst-{id:020}.tmp"));
    let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
    let mut index = Vec::new();
    for (pos, entry) in entries.iter().enumerate() {
        let offset = file.stream_position()?;
        if pos % INDEX_STRIDE == 0 {
            index.push(IndexEntry { key: entry.record.key.clone(), offset });
        }
        write_frame(&mut file, entry)?;
    }
    let max_index = entries.iter().map(|entry| entry.index).max().unwrap_or(0);
    let bloom = BloomFilter::new(entries.iter().map(|entry| entry.record.key.clone()), entries.len());
    let footer = SstableFooter { index: index.clone(), bloom: bloom.clone(), max_index };
    let footer_bytes = bincode::serialize(&footer).map_err(io::Error::other)?;
    file.write_all(&footer_bytes)?;
    file.write_all(&(footer_bytes.len() as u64).to_be_bytes())?;
    file.write_all(MAGIC)?;
    file.sync_all()?;
    fs::rename(&tmp, &path)?;
    sync_dir(dir);
    Ok(Sstable { id, path, index, bloom, max_index })
}

fn load_tables(dir: &Path) -> io::Result<Vec<Sstable>> {
    let mut tables = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
            continue;
        }
        let Some(id) = path.file_stem().and_then(|stem| stem.to_str()).and_then(|stem| stem.strip_prefix("sst-")).and_then(|id| id.parse().ok()) else {
            continue;
        };
        let footer = read_footer(&path)?;
        tables.push(Sstable { id, path, index: footer.index, bloom: footer.bloom, max_index: footer.max_index });
    }
    Ok(tables)
}

fn read_footer(path: &Path) -> io::Result<SstableFooter> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len < 16 {
        return Err(io::Error::new(ErrorKind::InvalidData, "sstable too small"));
    }
    file.seek(SeekFrom::End(-8))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(ErrorKind::InvalidData, "bad sstable magic"));
    }
    file.seek(SeekFrom::End(-16))?;
    let mut len_bytes = [0; 8];
    file.read_exact(&mut len_bytes)?;
    let footer_len = u64::from_be_bytes(len_bytes);
    file.seek(SeekFrom::Start(len - 16 - footer_len))?;
    let mut bytes = vec![0; footer_len as usize];
    file.read_exact(&mut bytes)?;
    bincode::deserialize(&bytes).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
}

fn lookup_table(table: &Sstable, key: &str) -> io::Result<Option<Option<String>>> {
    let start = table
        .index
        .partition_point(|entry| entry.key.as_str() <= key)
        .saturating_sub(1);
    let mut file = File::open(&table.path)?;
    if let Some(entry) = table.index.get(start) {
        file.seek(SeekFrom::Start(entry.offset))?;
    }
    let data_end = data_end(&table.path)?;
    while file.stream_position()? < data_end {
        let entry: DataEntry = match read_frame(&mut file)? {
            Some(entry) => entry,
            None => break,
        };
        match entry.record.key.as_str().cmp(key) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Equal => return Ok(Some(entry.record.value)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

fn read_all_entries(path: &Path) -> io::Result<Vec<DataEntry>> {
    let mut file = File::open(path)?;
    let end = data_end(path)?;
    let mut entries = Vec::new();
    while file.stream_position()? < end {
        if let Some(entry) = read_frame(&mut file)? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn data_end(path: &Path) -> io::Result<u64> {
    let len = fs::metadata(path)?.len();
    let mut file = File::open(path)?;
    file.seek(SeekFrom::End(-16))?;
    let mut bytes = [0; 8];
    file.read_exact(&mut bytes)?;
    Ok(len - 16 - u64::from_be_bytes(bytes))
}

fn read_wal_frames_truncating(path: &Path) -> io::Result<Vec<WalFrame>> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut frames = Vec::new();
    let mut good = 0;
    loop {
        let start = file.stream_position()?;
        match read_frame::<WalFrame>(&mut file) {
            Ok(Some(frame)) => {
                frames.push(frame);
                good = file.stream_position()?;
            }
            Ok(None) => break,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                file.set_len(good)?;
                break;
            }
            Err(err) => {
                if file.metadata()?.len() == start {
                    break;
                }
                return Err(err);
            }
        }
    }
    Ok(frames)
}

fn write_frame<T: Serialize>(file: &mut File, value: &T) -> io::Result<()> {
    let bytes = bincode::serialize(value).map_err(io::Error::other)?;
    let len = u32::try_from(bytes.len()).map_err(|_| io::Error::new(ErrorKind::InvalidInput, "frame too large"))?;
    file.write_all(&len.to_be_bytes())?;
    file.write_all(&bytes)
}

fn read_frame<T: for<'de> Deserialize<'de>>(file: &mut File) -> io::Result<Option<T>> {
    let mut len = [0; 4];
    match file.read_exact(&mut len) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
            if len == [0; 4] {
                return Ok(None);
            }
            return Err(err);
        }
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len) as usize;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    bincode::deserialize(&bytes).map(Some).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
}

fn record_size(record: &Record) -> usize {
    record.key.len() + record.value.as_ref().map_or(0, String::len) + 16
}

fn hash_with_seed(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn sync_dir(path: &Path) {
    if let Ok(dir) = File::open(path) {
        let _ = dir.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> LsmOptions {
        LsmOptions { memtable_bytes: 64, compaction_threshold: 10 }
    }

    #[test]
    fn memtable_flush_triggers_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut tree = LsmTree::open(dir.path(), opts()).unwrap();
        tree.apply(1, &Command::Set { key: "a".into(), value: "x".repeat(80) }).unwrap();
        assert_eq!(tree.sstable_count(), 1);
        assert_eq!(tree.get_mut("a").unwrap(), Some("x".repeat(80)));
    }

    #[test]
    fn recovery_replays_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
            tree.apply(1, &Command::Set { key: "k".into(), value: "v".into() }).unwrap();
        }
        let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
        assert_eq!(tree.last_applied(), 1);
        assert_eq!(tree.get_mut("k").unwrap(), Some("v".into()));
    }

    #[test]
    fn recovery_replays_wal_after_crash_between_append_and_memtable_apply() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
            tree.append_wal_only_for_test(1, &Command::Set { key: "k".into(), value: "v".into() }).unwrap();
        }
        let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
        assert_eq!(tree.get_mut("k").unwrap(), Some("v".into()));
    }

    #[test]
    fn torn_wal_tail_is_truncated_and_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
            tree.apply(1, &Command::Set { key: "ok".into(), value: "yes".into() }).unwrap();
        }
        let mut wal = OpenOptions::new().append(true).open(dir.path().join("wal.log")).unwrap();
        wal.write_all(&99_u32.to_be_bytes()).unwrap();
        wal.write_all(b"partial").unwrap();
        wal.sync_all().unwrap();
        let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
        assert_eq!(tree.get_mut("ok").unwrap(), Some("yes".into()));
    }

    #[test]
    fn bloom_filter_skips_unnecessary_disk_reads() {
        let dir = tempfile::tempdir().unwrap();
        let mut tree = LsmTree::open(dir.path(), opts()).unwrap();
        tree.apply(1, &Command::Set { key: "present".into(), value: "v".repeat(80) }).unwrap();
        let before = tree.disk_read_count();
        assert_eq!(tree.get_mut("definitely-missing").unwrap(), None);
        assert_eq!(tree.disk_read_count(), before);
    }

    #[test]
    fn compaction_merges_and_removes_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let mut tree = LsmTree::open(dir.path(), LsmOptions { memtable_bytes: 1, compaction_threshold: 10 }).unwrap();
        tree.apply(1, &Command::Set { key: "gone".into(), value: "v".into() }).unwrap();
        tree.apply(2, &Command::Delete { key: "gone".into() }).unwrap();
        tree.compact().unwrap();
        assert_eq!(tree.get_mut("gone").unwrap(), None);
        assert_eq!(tree.sstable_count(), 0);
    }

    #[test]
    fn restart_tolerates_compaction_output_plus_old_tables() {
        let dir = tempfile::tempdir().unwrap();
        let old_paths;
        {
            let mut tree = LsmTree::open(dir.path(), LsmOptions { memtable_bytes: 1, compaction_threshold: 10 }).unwrap();
            tree.apply(1, &Command::Set { key: "k".into(), value: "old".into() }).unwrap();
            tree.apply(2, &Command::Set { key: "k".into(), value: "new".into() }).unwrap();
            old_paths = tree.tables.iter().map(|t| t.path.clone()).collect::<Vec<_>>();
            let mut merged = BTreeMap::new();
            for table in &tree.tables {
                for entry in read_all_entries(&table.path).unwrap() {
                    merged.entry(entry.record.key.clone()).or_insert(entry);
                }
            }
            let entries = merged.into_values().collect::<Vec<_>>();
            let _ = write_table(dir.path(), 99, &entries).unwrap();
        }
        for path in old_paths {
            assert!(path.exists());
        }
        let mut tree = LsmTree::open(dir.path(), LsmOptions::default()).unwrap();
        assert_eq!(tree.get_mut("k").unwrap(), Some("new".into()));
    }
}
