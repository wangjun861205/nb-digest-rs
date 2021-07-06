use bytes::Bytes;
use futures::StreamExt;
use nb_chunk_stream_rs::ChunkStream;
use num_cpus::get;
use sha2::Digest;
use std::sync::mpsc::channel;
use std::thread::spawn;

pub struct Digester<D, E>
where
    D: Digest + Send + Sync + Clone,
    E: std::error::Error + Send + Sync + 'static,
{
    digest: D,
    stream: ChunkStream<E>,
}

impl<D, E> Digester<D, E>
where
    D: Digest + Send + Sync + Clone + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(digest: D, stream: ChunkStream<E>) -> Self {
        Self { digest, stream }
    }
    pub async fn calc(mut self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let num_of_cpu = get();
        let mut txs = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..num_of_cpu {
            let (tx, rx) = channel::<Bytes>();
            let mut digester = self.digest.clone();
            txs.push(tx);
            handles.push(spawn(move || {
                while let Ok(bs) = rx.recv() {
                    digester.update(&bs);
                }
                digester.finalize()
            }));
        }
        let mut enumerate = self.stream.enumerate();
        while let Some((i, res)) = enumerate.next().await {
            match res {
                Ok(bs) => {
                    if let Err(e) = txs[i % num_of_cpu].send(bs) {
                        return Err(Box::new(e));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        drop(txs);
        for h in handles {
            let bs = h.join().unwrap();
            self.digest.update(&bs);
        }
        Ok(self.digest.finalize().to_vec())
    }
}

#[tokio::test]
async fn test_digest() {
    use bytes::{BufMut, BytesMut};
    use futures::stream::iter;
    use sha2::Sha256;
    let mut l = Vec::new();
    for _ in 0..3 {
        let mut buf = BytesMut::new();
        for j in 0..255 {
            buf.put_u8(j as u8);
        }
        l.push(buf.freeze());
    }
    let s = iter(l).map(|v| Ok::<Bytes, std::convert::Infallible>(v));
    let mut cs = ChunkStream::with_capacity(Box::new(s), 100);
    let d = Digester::new(Sha256::new(), cs);
    println!("{:?}", d.calc().await.unwrap());
}
