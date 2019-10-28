use std::path::{Path, PathBuf};
use std::sync::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry::*;
use std::fs::{OpenOptions, File};

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE];

// T is for id type
pub struct Storage {
  base: PathBuf,
  files: HashMap<u64, StorageFile>,
  idmap: HashMap<(String, String), u64>,
}

impl Storage {
  pub fn new<P: AsRef<Path>>(base: P) -> Storage {
    let mut p = PathBuf::new();
    p.push(base.as_ref());
    Storage {
      base: p,
      files: Default::default(),
      idmap: Default::default(),
    }
  }

  pub fn open<D: AsRef<str>, T: AsRef<str>>(&mut self, database: D, table: T) -> Result<StorageFile, std::io::Error> {
    use rand::Rng;

    let idkey = (database.as_ref().to_owned(), table.as_ref().to_owned());
    match self.idmap.entry(idkey) {
      Occupied(i) => Ok(self.files.get(i.get()).cloned().expect("Storage maps out-of-sync")),
      Vacant(v) => {
        let dir = self.base.join(database.as_ref());
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(table.as_ref());
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;

        let id: u64 =  rand::thread_rng().gen();
        let sf = StorageFile { inner: Arc::new(Mutex::new(file)), id };

        self.files.insert(id, sf.clone());
        v.insert(id);

        Ok(sf)
      },
    }
  }

  pub fn get(&self, id: u64) -> Option<StorageFile> {
    self.files.get(&id).cloned()
  }
}

#[derive(Clone)]
pub struct StorageFile {
  id: u64,
  inner: Arc<Mutex<File>>,
}

impl StorageFile {
  pub fn occupy(&mut self) -> MutexGuard<File> {
    self.inner.lock().unwrap()
  }

  pub fn write_page(&mut self, at: usize, page: &Page) -> Result<(), std::io::Error> {
    use std::io::SeekFrom;
    use std::io::Seek;
    use std::io::Write;

    let mut file = self.occupy();
    file.seek(SeekFrom::Start((at * PAGE_SIZE) as u64))?;
    file.write(page)?;

    Ok(())
  }

  pub fn read_page<'a, 'b: 'a>(&'a mut self, at: usize, buf: &'b mut Page) -> Result<&'b mut Page, std::io::Error> {
    use std::io::SeekFrom;
    use std::io::Seek;
    use std::io::Read;

    let mut file = self.occupy();
    file.seek(SeekFrom::Start((at * PAGE_SIZE) as u64))?;
    file.read_exact(buf)?;

    Ok(buf)
  }
}
