use criterion::{black_box, criterion_group, criterion_main, Criterion};
use idhash::unf_vector::sigfig;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sigfig", |b| b.iter(|| sigfig(black_box(20.0), 2)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
