package collect

var lsnStartQuery string = "select pg_current_wal_insert_lsn() pg_current_wal_insert_lsn"
var getEngineMajorVersion string = "select split_part(split_part(version(), ' ', 2),'.',1)::int engine_major_version"
var snapshotTime string = "extract(epoch from now())::bigint snapshot_time"
var hammer = map[string]string{
	"hammer_orders":     "select  %s, count(*) count from orders",
	"hammer_new_orders": "select  %s, count(*) count from new_order",
}

var activity = map[string]string{
	"pg_stat_activity": `
	select
		 %s,
		 a.*,
		 (pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'%s'))::bigint wal_generated,
		 txid_current() current_txid,
		 cardinality(pg_blocking_pids(pid)) blocking_pids
	from pg_stat_activity a
	`,
	"pg_stat_replication": "select %s, application_name, extract(epoch from replay_lag)::float replay_lag from pg_stat_replication",
}

var statsInfo = map[string]string{
	"pg_version": "select %s, '%s' host, '%s' dbname, version() engine_version",
}

var stats13 = map[string]string{
	"pg_stat_database":      "select %s, a.* from pg_stat_database a",
	"pg_stat_all_indexes":   "select %s, a.* from pg_stat_all_indexes a",
	"pg_stat_all_tables":    "select %s, a.* from pg_stat_all_tables a",
	"pg_statio_all_indexes": "select %s, a.* from pg_statio_all_indexes a",
	"pg_statio_all_tables":  "select %s, a.* from pg_statio_all_tables a",
	"pg_stat_bgwriter":      "select %s, a.* from pg_stat_bgwriter a",
	"pg_stat_statements": `
							  select %s, 
							  	substr(query,1,25) as query,
							  	queryid,
							  	dbid,
							  	userid,
							  	total_exec_time,
							  	rows,
							  	calls,
							  	shared_blks_hit,
							  	shared_blks_read,
							  	shared_blks_dirtied,
							  	shared_blks_written,
							  	temp_blks_read,
							  	temp_blks_written,
							  	local_blks_read,
							  	local_blks_written,
							  	wal_fpi
							  	from pg_stat_statements`,
}

var stats12 = map[string]string{
	"pg_stat_database":      "select %s, a.* from pg_stat_database a",
	"pg_version":            "select %s, '%s' host, '%s' dbname, version() engine_version",
	"pg_stat_all_indexes":   "select %s, a.* from pg_stat_all_indexes a",
	"pg_stat_all_tables":    "select %s, a.* from pg_stat_all_tables a",
	"pg_statio_all_indexes": "select %s, a.* from pg_statio_all_indexes a",
	"pg_statio_all_tables":  "select %s, a.* from pg_statio_all_tables a",
	"pg_stat_bgwriter":      "select %s, a.* from pg_stat_bgwriter a",
	"pg_stat_statements": `
							  select %s,
								substr(query,1,25) as query,
								queryid,
								dbid,
								userid,
								total_exec_time,
								rows,
								calls,
								shared_blks_hit,
								shared_blks_read,
								shared_blks_dirtied,
								shared_blks_written,
								temp_blks_read,
								temp_blks_written,
								local_blks_read,
								local_blks_written
								from pg_stat_statements`,
}
