-- --------------------------------------------------
-- Purpose: Get perfmon-like metrics from OS DMV so that Performance Monitor does not need to be used
-- Author: Patrick El-Azem, adapted from Justin Henriksen (https://justinhenriksen.wordpress.com/)
-- Date: 2016-06-15
-- Notes
-- This returns data for a specific point in time. To get data over a range of time at intervals, run it accordingly from an external execution context and append the output to your destination, e.g. CSV file.
-- This has been tested on SQL Server 2014. It also runs on Azure SQL DB v12, though the instance name used there is a GUID and so some of the metrics are not extracted.
--		However, that's OK since the point of this script is to run on SQL Server 2014 to get perf metrics for Azure DB DTU calculator input build - not to execute on Azure SQL DB.
-- --------------------------------------------------

declare
	@now					datetime = getutcdate(),
	@dbName					nvarchar(50) = db_name(),
	@instanceName			nvarchar(50) = 'internal',
	@objResPoolStats		nvarchar(50) = '%:Resource Pool Stats',
	@ctrCPUUsagePct			nvarchar(50) = 'CPU usage %',
	@ctrDiskReadIO			nvarchar(50) = 'Disk Read IO/sec',
	@ctrDiskReadBytes		nvarchar(50) = 'Disk Read Bytes/sec',
	@ctrDiskWriteIO			nvarchar(50) = 'Disk Write IO/sec',
	@ctrDiskWriteBytes		nvarchar(50) = 'Disk Write Bytes/sec',
	@ctrLogBytesFlushed		nvarchar(50) = 'Log Bytes Flushed/sec'
;

select
	[Date/Time] = @now,
	pivoted.*
from 
(
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(object_name) like @objResPoolStats and
		rtrim(instance_name) = @instanceName and
		rtrim(counter_name) = @ctrCPUUsagePct
	union all
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(instance_name) = @instanceName and
		rtrim(counter_name) = @ctrDiskReadIO
	union all
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(instance_name) = @instanceName and
		rtrim(counter_name) = @ctrDiskReadBytes
	union all
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(instance_name) = @instanceName and
		rtrim(counter_name) = @ctrDiskWriteIO
	union all
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(instance_name) = @instanceName and
		rtrim(counter_name) = @ctrDiskWriteBytes
	union all
	select
		counter_name = rtrim(counter_name),
		cntr_value = coalesce(cntr_value, 0)
	from
		sys.dm_os_performance_counters
	where
		rtrim(instance_name) = @dbName and
		rtrim(counter_name) = @ctrLogBytesFlushed
) as orig
pivot
	(sum(cntr_value) for counter_name in ([CPU usage %], [Disk Read IO/sec], [Disk Read Bytes/sec], [Disk Write IO/sec], [Disk Write Bytes/sec], [Log Bytes Flushed/sec]))
as pivoted
;