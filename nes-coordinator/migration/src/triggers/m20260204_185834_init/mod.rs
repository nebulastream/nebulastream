use sea_orm::DbBackend;

const SQLITE_UP: &str = include_str!("sqlite.up.sql");
const SQLITE_DOWN: &str = include_str!("sqlite.down.sql");

pub fn up(backend: DbBackend) -> Option<&'static str> {
    match backend {
        DbBackend::Sqlite => Some(SQLITE_UP),
        _ => None,
    }
}

pub fn down(backend: DbBackend) -> Option<&'static str> {
    match backend {
        DbBackend::Sqlite => Some(SQLITE_DOWN),
        _ => None,
    }
}
