use axum::{routing::get, Router};
use axum_test::TestServer;
use chrono::{Duration, Timelike, Utc};
use rsqueue::*;
use uuid::Uuid;

fn totals(points: &[TimeSeriesPoint]) -> (u64, u64, u64) {
    points.iter().fold((0, 0, 0), |sum, point| {
        (
            sum.0 + point.enqueued,
            sum.1 + point.dequeued,
            sum.2 + point.deleted,
        )
    })
}

#[tokio::test]
async fn history_tracks_deliveries_separately_from_successful_acknowledgements() {
    let storage = tempfile::tempdir().unwrap();
    let state = AppState::new(storage.path().to_path_buf());
    let mut queue = Queue::new("work".into(), 120, true, 300, None, None, None);
    queue.enqueue("first".into(), None, None, None).unwrap();
    queue.enqueue("second".into(), None, None, None).unwrap();
    assert!(queue.enqueue("first".into(), None, None, None).is_err());
    let delivery = queue.dequeue(2).messages;
    let first_handle = delivery[0].receipt_handle.unwrap();
    assert!(queue.nack_message(first_handle));
    let redelivery = queue.dequeue(1).messages;
    assert!(!queue.delete_message(first_handle));
    assert!(queue.delete_message(redelivery[0].receipt_handle.unwrap()));
    let second_handle = delivery[1].receipt_handle.unwrap();
    queue.batch_delete_messages(vec![second_handle, second_handle, Uuid::new_v4()]);
    queue.purge(); // Purging must not count as acknowledgement or clear history.
    state.queues.write().await.insert("work".into(), queue);

    let mut other = Queue::new("other".into(), 120, false, 300, None, None, None);
    other.enqueue("pending".into(), None, None, None).unwrap();
    state.queues.write().await.insert("other".into(), other);

    let server = TestServer::new(
        Router::new()
            .route("/stats/history", get(get_global_history))
            .route("/queues/:name/stats/history", get(get_queue_history))
            .with_state(state),
    )
    .unwrap();

    for minutes in [15, 60, 360, 1440] {
        let granularity = if minutes <= 60 { 60 } else { 300 };
        let query = format!("minutes={minutes}&granularity_seconds={granularity}");
        let response = server
            .get("/queues/work/stats/history")
            .add_raw_query_param(&query)
            .await;
        response.assert_status_ok();
        let history: QueueTimeSeries = response.json();
        assert_eq!(totals(&history.data_points), (2, 3, 2));
        assert_eq!(history.from, history.data_points[0].timestamp);
        assert_eq!(history.from.second(), 0);
        assert_eq!(history.from.nanosecond(), 0);
        assert_eq!(history.data_points.len(), minutes * 60 / granularity + 1);

        let global: GlobalTimeSeries = server
            .get("/stats/history")
            .add_raw_query_param(&query)
            .await
            .json();
        assert_eq!(totals(&global.data_points), (3, 3, 2));
    }
    // Reopening the dashboard reads the same server history.
    let again: QueueTimeSeries = server.get("/queues/work/stats/history").await.json();
    assert_eq!(totals(&again.data_points), (2, 3, 2));
    server
        .get("/queues/missing/stats/history")
        .await
        .assert_status_not_found();
}

#[tokio::test]
async fn history_rejects_invalid_or_unbounded_windows() {
    let storage = tempfile::tempdir().unwrap();
    let state = AppState::new(storage.path().to_path_buf());
    let server = TestServer::new(
        Router::new()
            .route("/stats/history", get(get_global_history))
            .route("/queues/:name/stats/history", get(get_queue_history))
            .with_state(state),
    )
    .unwrap();
    for query in [
        "minutes=0",
        "minutes=1441",
        "minutes=18446744073709551615",
        "granularity_seconds=0",
        "granularity_seconds=1",
        "granularity_seconds=90",
        "granularity_seconds=86460",
        "granularity_seconds=18446744073709551615",
    ] {
        for path in ["/stats/history", "/queues/work/stats/history"] {
            server
                .get(path)
                .add_raw_query_param(query)
                .await
                .assert_status_bad_request();
        }
    }
    let history: GlobalTimeSeries = server.get("/stats/history").await.json();
    assert_eq!(history.data_points.len(), 61);
    assert_eq!(totals(&history.data_points), (0, 0, 0));
}

#[test]
fn history_aggregates_minutes_and_idle_intervals_without_double_counting() {
    let start = Utc::now()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let mut tracker = TimeSeriesTracker::new(24);
    for (minute, count, depth_sum, depth_count) in [(0, 12, 30, 2), (4, 18, 30, 1), (5, 6, 0, 1)] {
        tracker.buckets.insert(
            start + Duration::minutes(minute),
            TimeBucketStats {
                enqueued: count,
                dequeued: count,
                deleted: count / 2,
                queue_depth_sum: depth_sum,
                queue_depth_count: depth_count,
            },
        );
    }
    let points = tracker.get_data_points(start, start + Duration::minutes(10), 300);
    assert_eq!(points.len(), 3);
    assert_eq!(points[0].dequeued, 30);
    assert_eq!(points[0].queue_depth, 20); // Weighted average, not average of averages.
    assert_eq!(points[1].dequeued, 6);
    assert_eq!(points[2].dequeued, 0);
    assert_eq!(totals(&points), (36, 36, 18));
    assert!(tracker.get_data_points(start, start, 0).is_empty());
    assert!(tracker.get_data_points(start, start, 90).is_empty());
}

#[test]
fn history_keeps_only_retained_buckets_during_heavy_traffic() {
    let now = Utc::now()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let mut tracker = TimeSeriesTracker::new(24);
    tracker
        .buckets
        .insert(now - Duration::hours(25), TimeBucketStats::default());
    tracker
        .buckets
        .insert(now - Duration::hours(23), TimeBucketStats::default());
    for _ in 0..10_000 {
        tracker.record_enqueue(20);
    }
    assert!(!tracker.buckets.contains_key(&(now - Duration::hours(25))));
    assert!(tracker.buckets.contains_key(&(now - Duration::hours(23))));
    assert!(tracker.buckets.len() <= 3); // Allows crossing a minute boundary during the test.
    assert_eq!(
        tracker.buckets.values().map(|s| s.enqueued).sum::<u64>(),
        10_000
    );
    assert_eq!(
        tracker
            .buckets
            .values()
            .map(|s| s.queue_depth_sum)
            .sum::<u128>(),
        200_000
    );
}
