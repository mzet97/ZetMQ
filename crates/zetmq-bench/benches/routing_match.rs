use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use zetmq_core::{RoutingEngine, Subject, SubjectPattern, SubscriptionId};

fn bench_exact_match(c: &mut Criterion) {
    let engine = RoutingEngine::new();
    let pattern = SubjectPattern::parse("orders.created").unwrap();
    engine.insert(&pattern, SubscriptionId::new(1));

    let subject = Subject::parse("orders.created").unwrap();

    c.bench_function("exact_match_single", |b| {
        b.iter(|| engine.match_subject(black_box(&subject)))
    });
}

fn bench_wildcard_match(c: &mut Criterion) {
    let engine = RoutingEngine::new();
    for i in 0..1000u64 {
        let pattern = SubjectPattern::parse("orders.*").unwrap();
        engine.insert(&pattern, SubscriptionId::new(i));
    }

    let subject = Subject::parse("orders.created").unwrap();
    c.bench_function("wildcard_match_1000_subs", |b| {
        b.iter(|| engine.match_subject(black_box(&subject)))
    });
}

fn bench_many_subscriptions(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_scaling");

    for count in [10, 100, 1000, 10000] {
        let engine = RoutingEngine::new();
        for i in 0..count {
            let pattern = SubjectPattern::parse(&format!("subject.{i}")).unwrap();
            engine.insert(&pattern, SubscriptionId::new(i as u64));
        }

        let subject = Subject::parse(&format!("subject.{}", count / 2)).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| engine.match_subject(black_box(&subject)))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_exact_match,
    bench_wildcard_match,
    bench_many_subscriptions
);
criterion_main!(benches);
