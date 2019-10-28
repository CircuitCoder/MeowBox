use std::sync::*;

use crate::storage::{Page, PAGE_SIZE, StorageFile};
use std::iter::Iterator;

enum FieldType {
  Int,
  Str(usize),
}

impl FieldType {
  fn len(&self) -> usize {
    match *self {
      Self::Int => 8,
      Self::Str(s) => s,
    }
  }
}

struct Schema {
  fields: Vec<FieldType>
}

impl Schema {
  fn len(&self) -> usize {
    self.fields.iter().fold(0, |acc, c| acc + c.len())
  }
}

struct Record<'a> {
  buf: &'a [u8],
  schema: &'a Schema,
}

struct Cursor<'a> {
  schema: Schema,

  manager: &'a mut Manager,
  cur_page: Option<usize>,
  cur_buf: Page,
  cur_size: usize,
  cur_idx: usize,

  ended: bool,
}

impl<'a> Cursor<'a> {
  fn next<'b>(&'b mut self) -> Option<Record<'b>> {
    if self.ended { return None; }
    if self.cur_idx == self.cur_size {
      self.cur_idx = 0;
      let next = self.cur_page.map(|i| i+1).unwrap_or(0);
      self.cur_page = Some(next);

      // TODO: ensure all record has finished processing
      match self.manager.read_page(next, &mut self.cur_buf) {
        Some(l) => self.cur_size = l,
        None => {
          self.ended = true;
          return None;
        }
      }
    }

    let len = self.schema.len();
    let range = (self.cur_idx * len) .. ((self.cur_idx+1) * len);
    let result = Record {
      buf: &self.cur_buf[range],
      schema: &self.schema,
    };

    self.cur_idx += 1;
    Some(result)
  }
}

struct Manager {
  storage: StorageFile,
  occupied: Vec<usize>,
}

impl Manager {
  pub fn new(storage: StorageFile, occupied: Vec<usize>) -> Manager {
    Manager { storage, occupied }
  }

  pub fn read_page(&mut self, at: usize, buf: &mut Page) -> Option<usize> {
    let occupied = self.occupied.get(at).cloned();
    if occupied.is_some() {
      self.storage.read_page(at, buf);
    }
    return occupied;
  }

  fn cursor<'a>(&'a mut self, schema: Schema) -> Cursor<'a> {
    Cursor {
      schema: schema,

      cur_page: None,
      cur_buf: [0; PAGE_SIZE],
      cur_size: 0,
      cur_idx: 0,

      manager: self,

      ended: false,
    }
  }
}
