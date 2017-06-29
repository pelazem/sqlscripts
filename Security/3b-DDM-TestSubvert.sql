set nocount on;

-- Revert to default user, member of db_owner
revert;

-- Impersonate a user who is not in db_owner and does not have UNMASK
execute as user = 'SqlSalesPerson1';

-- Create a temp table which I'll use to try to get around DDM
create table #SSNs ([SSN] [nvarchar](20) null);

-- Insert SSNs from our masked table into the temp table
insert into #SSNs(SSN) select SSN from data.Users;

-- Select data from our temp table. It will be masked, as expected.
select top 5 * from #SSNs;

-- Back to default user, member of db_owner
revert;

-- The temp table contains statically masked data, so even db_owner member will see masked data
select top 5 * from #SSNs; 

-- Verify that same user (db_owner, therefore with UNMASK) does see source table in the clear
select top 5 SSN from data.Users;

-- Cleanup temp table
drop table #SSNs;
go