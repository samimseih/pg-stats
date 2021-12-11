package report

type reportSections map[int]map[string]map[string]map[string]string

var html string = `
<!DOCTYPE html>
<head>
	<style type="text/css">

	/* Style the tab */
	.tab {
  		overflow: hidden;
	}

	/* Style the buttons inside the tab */
	.tab button {
  		float: left;
  		outline: none;
  		cursor: pointer;
	}

	/* Change background color of buttons on hover */
	.tab button:hover {
  		background-color: #ddd;
	}

	/* Create an active/current tablink class */
	.tab button.active {
  		background-color: #ccc;
	}

	/* Style the tab content */
	.tabcontent caption {
		background-color: #33CCFF;
	}

	.tabcontent th {
		 background-color: #D3D3D3;
	}

	.tabcontent table td {
		background-color: #F5F5F5;
	}

	/* Style the header */

	.theader th {
		 background-color: #D3D3D3;
	}

	.theader table td {
		background-color: #F5F5F5;
	}

	</style>
	<script>
		function clickHandle(evt, tableName) {
  		let i, tabcontent, tablinks;

  		// This is to clear the previous clicked content.
  		tabcontent = document.getElementsByClassName("tabcontent");
  		for (i = 0; i < tabcontent.length; i++) {
    		tabcontent[i].style.display = "none";
  		}

  		// Set the tab to be "active".
  		tablinks = document.getElementsByClassName("tablinks");
  		for (i = 0; i < tablinks.length; i++) {
    		tablinks[i].className = tablinks[i].className.replace(" active", "");
  		}

  		// Display the clicked tab and set it to active.
  		document.getElementById(tableName).style.display = "block";
  		evt.currentTarget.className += " active";
		}
	</script>
</head>
<body>
	{header1}
	<div class="tab">
		<table>
			<tr>
  				<td><button class="tablinks" onclick="clickHandle(event, 'instance_activity')">Instance Activity</button></td>
  			</tr>
  		</table>
	</div>
	<div id="instance_activity" class="tabcontent">
		<table>
   			<tr>
   				<td valign="top">{top_wait_events}</td>
   				<td valign="top">
   					<table>
   						<tr>
   							<td valign="top">{dml1}</th>
   						</tr>
   						<tr>
   							<td valign="top">{checkpointing1}</td>
   						</tr>
   						<tr>
   							<td valign="top">{tps}</td>
   						</tr>
   						<tr>
   							<td valign="top">{wal_generated}</td>
   						</tr>
   						<tr>
   							<td valign="top">{replica_lag}</td>
   						</tr>
   						<tr>
   							<td style="{style_wal_fpi}" valign="top">{wal_fpi}</td>
   						</tr>
   					</table>
   				</td>
   			</tr>
   		</table>
   	</div>
</body>
`

var sections = reportSections{
	0: {
		"pg_stat_database": {
			"nodisplay": {
				"query": `
				create table pg_database_all as select distinct datid, datname from pg_stat_database
				`,
				"html_class": "theader",
				"title":      "",
				"render":     "f",
			},
			"header1": {
				"query": `
					select 
						'{host}' Host, 
						'{dbname}' Database , 
						'{engine_version}' engine_version,
						case when '{start}' == '0' then datetime(min(snapshot_time), 'unixepoch', 'utc') else datetime('{start}', 'unixepoch', 'localtime') end "Start Time",
						case when '{end}' == '0' then datetime(max(snapshot_time), 'unixepoch', 'utc') else datetime('{end}', 'unixepoch', 'localtime') end "End Time"
					from {metric}
				`,
				"html_class": "theader",
				"title":      "",
				"render":     "t",
			},
			"dml1": {
				"query": `
        	select
        		datname,
            	datid,
                sum(tup_inserted) "tuples inserted",
                sum(tup_updated) "tuples updated",
                sum(tup_deleted) "tuples deleted",
                printf("%.2f", sum(tup_inserted)*1.0/(max(snapshot_time) - min(snapshot_time))) "inserts per second",
                printf("%.2f", sum(tup_updated)*1.0/(max(snapshot_time) - min(snapshot_time))) "updates per second",
                printf("%.2f", sum(tup_deleted)*1.0/(max(snapshot_time) - min(snapshot_time))) "deletes per second"
            from
            (
            	select
                	datid,
                    datname,
                    tup_inserted - lag(tup_inserted) over (partition by datname order by datname,snapshot_time) tup_inserted,
                    tup_updated - lag(tup_updated) over (partition by datname order by datname,snapshot_time) tup_updated,
                    tup_deleted - lag(tup_deleted) over (partition by datname order by datname,snapshot_time) tup_deleted,
                    snapshot_time
                from pg_stat_database
            )
            group by datid, datname
            order by sum(tup_inserted)+sum(tup_updated)+sum(tup_deleted) desc 
				`,
				"html_class": "",
				"title":      "DML",
				"render":     "t",
			},
		}},
	1: {
		"pg_stat_bgwriter": {
			"checkpointing1": {
				"query": `
            select
            	sum(checkpoints_timed) "checkpoints timed",
                sum(checkpoints_requested) "checkpoints requested",
                sum(buffers_checkpoint) "buffers checkpoint",
                sum(buffers_backend) "buffers backend",
                sum(buffers_clean) "buffers clean",
                printf("%.2f", (sum(checkpoints_timed)*1.0 + sum(checkpoints_requested))/(max(snapshot_time) - min(snapshot_time))) "checkpoints per second",
                printf("%.2f", (sum(checkpoints_timed)*1.0 + sum(checkpoints_requested))/(max(snapshot_time) - min(snapshot_time))*60) "checkpoints per minute"
            from (
            	select
                	checkpoints_timed - lag(checkpoints_timed) over (order by snapshot_time) checkpoints_timed,
                    checkpoints_req - lag(checkpoints_req) over (order by snapshot_time) checkpoints_requested,
                    buffers_checkpoint - lag(buffers_checkpoint) over  (order by snapshot_time) buffers_checkpoint,
                    buffers_backend - lag(buffers_backend) over  (order by snapshot_time) buffers_backend,
                    buffers_clean - lag(buffers_clean) over  (order by snapshot_time) buffers_clean,
                    snapshot_time
                from pg_stat_bgwriter
            )
				`,
				"html_class": "theader",
				"title":      "Checkpointing",
				"render":     "t",
			},
		}},
	2: {
		"pg_stat_activity": {
			"top_wait_events": {
				"query": `
			select 
				distinct
				a.datname,
				a.datid,
				cast(
					round(
						cast(count(*) over (partition by a.wait_event,a.wait_event_type, a.datid) as real) /
						cast(count(*) over (partition by 'X' ) as real),
						2
					)*100
				as int) "% of total activity", 
				case a.wait_event when '' then 'CPU' else wait_event end "wait event",
				case a.wait_event_type when '' then 'CPU' else a.wait_event_type end "wait event type"
			from pg_stat_activity a
			where a.state='active' 
			order by 3 desc
				`,
				"html_class": "",
				"title":      "Top Wait Events - Database",
				"render":     "t",
			},
			"tps": {
				"query": `
			select
			sum(diff_txid) "total transactions",
			printf("%.2f", sum(diff_txid)*1.0/(max(snapshot_time) - min(snapshot_time))) "transactions per second",
			printf("%.2f", sum(diff_txid)*1.0/(max(snapshot_time) - min(snapshot_time))*60) "transactions per minute"
			from
			(
				select
					current_txid - lag(current_txid) over (order by snapshot_time) diff_txid,
					snapshot_time
				from (
					select distinct snapshot_time, current_txid from pg_stat_activity
				)
			)
				`,
				"html_class": "",
				"title":      "Transaction Processing",
				"render":     "t",
			},
			"wal_generated": {
				"query": `
			select 
			printf("%.2f", (sum(diff_wg)*1.0/(max(snapshot_time) - min(snapshot_time)))/1024/1024) "(MB) wal generated per second",
			printf("%.2f", (sum(diff_wg)*1.0/(max(snapshot_time) - min(snapshot_time))/1024/1024)*60) "(MB) wal generated per minute"
			from (
			select wal_generated - lag(wal_generated)  over (order by snapshot_time) diff_wg, snapshot_time from pg_stat_activity
		)
				`,
				"html_class": "",
				"title":      "Wal generated",
				"render":     "t",
			},
		},
	},
	3: {
		"pg_stat_replication": {
			"replica_lag": {
				"query":      "select min(cast(replay_lag as int64)) min_replay_lag, max(cast(replay_lag as int64)) max_replay_lag, stddev(cast(replay_lag as int64)) stddev_replay_lag from pg_stat_replication",
				"html_class": "",
				"title":      "Replication Lag",
				"render":     "t",
			},
		},
	},
	4: {
		"pg_stat_statements": {
			"wal_fpi": {
				"query": `
				select case when sum(wal_fpis) < 0 then 0 else sum(wal_fpis) end total_fpi from (
					select wal_fpi - lag(wal_fpi) over ( partition by query, queryid, dbid, userid order by snapshot_time ) wal_fpis from pg_stat_statements
				)
				`,
				"html_class": "",
				"title":      "Full Page Images",
				"render":     "t",
				"minMajorVersion": "13",
			},
		},
	},
}
