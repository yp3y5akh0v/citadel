use criterion::{criterion_group, criterion_main};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod h2h;

criterion_group!(
    benches,
    h2h::count::bench,
    h2h::point::bench,
    h2h::scan::bench,
    h2h::filter::bench,
    h2h::sort::bench,
    h2h::join::bench,
    h2h::sum::bench,
    h2h::group_by::bench,
    h2h::insert::bench,
    h2h::update::bench,
    h2h::delete::bench,
    h2h::distinct::bench,
    h2h::union::bench,
    h2h::cte::bench,
    h2h::recursive_cte::bench,
    h2h::window_rank::bench,
    h2h::window_agg::bench,
    h2h::view_filter::bench,
    h2h::view_point::bench,
    h2h::correlated_exists::bench,
    h2h::correlated_in::bench,
    h2h::correlated_scalar::bench,
    h2h::insert_select::bench,
    h2h::savepoint_create::bench,
    h2h::savepoint_rollback::bench,
    h2h::savepoint_nested::bench,
    h2h::date_range_scan::bench,
    h2h::date_groupby::bench,
    h2h::date_extract::bench,
    h2h::date_sort::bench,
    h2h::date_arith::bench,
    h2h::upsert_all_new::bench,
    h2h::upsert_counter::bench,
    h2h::upsert_mixed::bench,
    h2h::upsert_dedup::bench,
    h2h::insert_returning::bench,
    h2h::update_returning::bench,
    h2h::delete_returning::bench,
    h2h::upsert_returning::bench,
    h2h::insert_gen_stored::bench,
    h2h::insert_gen_virtual::bench,
    h2h::update_gen_propagate::bench,
    h2h::select_gen_virtual::bench,
);
criterion_main!(benches);
