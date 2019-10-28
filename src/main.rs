mod storage;
mod record;

use storage::Storage;
use rand::Rng;

fn main() -> Result<(), std::io::Error> {
    let mut s = Storage::new("./store");
    let mut f = s.open("db", "tbl")?;
    
    let mut cont = [0; storage::PAGE_SIZE];
    let mut read = [0; storage::PAGE_SIZE];
    rand::thread_rng().fill(&mut cont);

    let page: usize = rand::thread_rng().gen_range(0, 16);

    f.write_page(page, &cont)?;
    f.read_page(page, &mut read)?;
    assert!(cont.iter().zip(read.iter()).all(|(a, b)| a == b));

    drop(f);
    s.close("db", "tbl")?;

    println!("File should have been explicitly closed");

    Ok(())
}
