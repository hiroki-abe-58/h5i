use calculator_kernel as k;
use calculator_server::executor_demo::{Calculator, CalculatorStore};
use h5i_app_pg::{Engine, EngineConfig, pool};
use std::{sync::{Arc, atomic::Ordering}, time::{Duration, SystemTime, UNIX_EPOCH}};

/// Whether some session holds the session-level advisory lock on `key`.
const HELD: &str = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted \
                    AND objsubid = 1 AND ((classid::bigint << 32) | objid::bigint) = $1)";

/// A write cancelled while it holds the tenant's advisory lock must not hand
/// the lock to the pool: before the fix, the connection went back to the pool
/// still holding it, and every later write for the tenant on another
/// connection waited for it indefinitely.
#[tokio::test]
async fn cancelled_write_releases_the_tenant_lock() {
    let Ok(url) = std::env::var("H5I_APP_TEST_DATABASE_URL") else { return; };
    let scoped = h5i_app_pg::with_schema(&url, "calculator_cancel").unwrap();
    let pg = Arc::new(Engine::<Calculator, CalculatorStore>::new(pool(&scoped, 4).unwrap(), EngineConfig::default()));
    pg.install_schema().await.unwrap();
    let org = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 % (1 << 50);
    let a = k::Principal { org, user: 1 };
    pg.execute(&a, &k::Command::Set { value: 1 }).await.unwrap().unwrap();

    // Hold the table, so the next write waits inside its attempt with the tenant lock taken.
    let (mut db, connection) = tokio_postgres::connect(&scoped, tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(connection);
    let tx = db.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE memories IN ACCESS EXCLUSIVE MODE").await.unwrap();
    let attempts = pg.stats.attempts.load(Ordering::Relaxed);
    let writer = { let pg = pg.clone(); tokio::spawn(async move { pg.execute(&a, &k::Command::Set { value: 2 }).await }) };
    tokio::time::timeout(Duration::from_secs(5), async {
        while pg.stats.attempts.load(Ordering::Relaxed) == attempts { tokio::task::yield_now().await; }
        while !tx.query_one(HELD, &[&(org as i64)]).await.unwrap().get::<_, bool>(0) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the writer took the tenant lock");
    tokio::time::sleep(Duration::from_millis(50)).await;
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();

    // The cancelled attempt's session is closed, so the lock is freed.
    let freed = tokio::time::timeout(Duration::from_secs(5), async {
        while db.query_one(HELD, &[&(org as i64)]).await.unwrap().get::<_, bool>(0) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await;
    assert!(freed.is_ok(), "the tenant lock stayed held after its write was cancelled");
    // And writes for the tenant go on.
    let next = tokio::time::timeout(Duration::from_secs(5), pg.execute(&a, &k::Command::Set { value: 3 })).await;
    assert_eq!(next.expect("a later write is not blocked").unwrap(), Ok(k::Reply::Value(3)));
    drop(db);
    connection.await.unwrap().unwrap();
}
