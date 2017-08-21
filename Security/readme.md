## Summary
These files implement a complete demonstration of the following SQL Server 2016 security features:

1.	Dynamic Data Masking (DDM) - see https://msdn.microsoft.com/library/mt130841.aspx
2.	Row-Level Security (RLS) - see https://msdn.microsoft.com/en-us/library/dn765131.aspx
3.	Transparent Data Encryption (TDE) - see https://msdn.microsoft.com/library/bb934049.aspx 

---

### Requirements:
1.	A _non-production_ SQL Server 2016 instance to run these scripts
2.	SQL Server Management Studio, latest release (download from https://msdn.microsoft.com/en-US/library/mt238290.aspx)

---

### Pre-Requisites
See _2-Database.sql_, lines 3-15. Two Windows groups and three Windows accounts are detailed.

The Windows groups are added to SQL database roles in this script to demonstrate security features.

If you do not want to use the Windows groups/users, comment out lines 95-104.

Alternately, find-replace these Windows group names in **all the SQL files**, substituting your own machine or domain name.

1.	PZVMW16SQL16\WinOpsMgrs
2.	PZVMW16SQL16\WinOpsPersons

---

### Important Notes - Yes, you should read these first
1. _2-Database.sql_ creates a database named TestDb. **This script will first DROP (delete) that TestDb database if it already exists - you have been warned.** See line 29.
2. _2-Database.sql_ creates database files for TestDb on lines 35 and 37. Please adjust the file system paths shown here to point to your SQL Server's data directory.

---

# How to use these files

### 1-Server.sql
This should be run first. It reconfigures the SQL Server for contained database authentication (see https://msdn.microsoft.com/en-us/library/ff929237.aspx).

Because this is a server-level setting, as usual... don't run on a production or important server, check with your DBAs first.

Configuring your server for contained database authentication enables all users to be created inside the database without server logins, making the database more easily portable.

If you do not configure your SQL Server for contained database authentication, then you will need to modify the next file to create server login-based database users.


### 2-Database.sql
This creates the test database (creatively named TestDb).

Run this as many times as needed. At each run, it will drop an already-existing instance of TestDb (though it will fail if you have open connections to the existing TestDb).

This script creates all database objects and sample data used in the various security feature scripts.

I recommend you re-run this script for a clean, consistent baseline before trying each of the features (i.e. don't run 3-, 4-, and 5- scripts in combination).


# DDM

### 3a-DDM-Setup.sql
This script modifies two tables to add masking. Various masking functions are used, depending on the column.

See the DDM documentation (link above) for masking function details.

### 3b-DDM-Test.sql
This script executes both ad-hoc `select` queries as well as stored procedures, to demonstrate that DDM works with both.

A number of blocks are executed here. The queries are the same each time; each block is executed as a different user to demonstrate masking in the result set.

Note that for our three non-dbo manager database roles (OpsMgrRole, SalesMgrRole, SupportMgrRole), the script first runs the queries "as is" to show masked output, then grants UNMASK to the manager role and re-runs the same queries to show how specific non-db_owner database principals can be excluded from data masking on a per-table basis. In each case, we reinstate masking afterward.

### 3b-DDM-TestSubvert.sql
This script attempts to subvert DDM by selecting data from a masked source table into a temp table, then selecting from that. The script proves that this does not subvert DDM.

### 3c-DDM-Remove.sql
This script simply removes the masking put in place in _3a-DDM-Setup.sql_. If you just re-run _2-Database.sql_ instead (which drops and re-creates the test database), then this script is not needed but it's here just in case.

# TDE
### 4a-TDE-Setup.sql
This script creates a server master encryption key and certificate. Modify the password and subject on lines 4 and 7 as needed.

While not mandatory, lines 11-17 immediately back up the newly created certificate and key to a file. This is a critical continuity practice, as database restore (e.g. after a disaster or hardware switch) will require this.

Lines 23-25 then create a database encryption key using the server certificate, then lines 28-29 activate TDE on the database.

### No test script
There is no Test script for TDE. However, you can back up this database, then transport the backup to a different SQL Server and attempt to restore.

### 4c-TDE-Remove.sql
This script removes TDE from the database. Note that lines 4-5, encryption removal, is a background process and you should run the check on lines 11-13 until it returns 1, indicating database decryption has completed.

Next, the database encryption key is removed.

Next, we switch to master and lines 22-25 delete the server certificate and master key created in _4a-TDE-Setup.sql_. OBVIOUSLY do not run these lines if you did not create the server certificate and master key for this test; as usual, do not run in production, check with your DBAs, be responsible and careful!


# RLS
### 5a-RLS-Setup.sql
This script implements row-level security on the data.Orders table. Per best practices (see RLS documentation above), a separate schema is created for RLS objects on line 4.

RLS for a table includes at least one predicate function, which can be a filter or block function. This script includes a filter predicate function, lines 7-25.

Lines 27-35 implement the actual security policy on the data.Orders table.

When a select is executed against data.Orders, the security policy invokes the predicate function for each row, and only outputs rows for which the predicate function returns a value of 1.

When does the predicate function return 1, i.e. the row is acceptable to return to the user?

In this case, when _any_ of the following conditions are true:

1.	The security policy passed a value of SalesRep _from the row currently being evaluated_ to the function, and that row's SalesRep value equals the currently executing user name - see line 30 in the policy, and line 15 in the predicate function (`@SalesRep = user_name()`).
	+	Essentially this means "is this row in the table MY row"
2.	The user is sa or dbo (`user_name() in ('sa', 'dbo')`)
3.	The user is in the SalesMgrRole database role
4.	The user is in the SupportMgrRole database role
5.	The user is in the OpsMgrRole database role

Note that the `is_member()` function, unlike `is_rolemember()`, can check database role membership both for users using Windows integrated authentication as well as SQL authentication. `is_rolemember()` only works for SQL principals.

Note also that in case an application is used that connects using an application credential rather than per-user credentials, the application can set a database session context key-value pair which the predicate function can read to do authorization checks.

### 5b-RLS-Test.sql
This script executes the same select query against the data.Orders table, running in turn as different users. In each case, a comment explains which rows we expect to see based on the RLS predicate, and why.

Note especially the blocks starting on lines 10 and 15. These run queries as Windows users; these users are not in our database at all. Rather, their containing Windows groups were added to our database, and these were added into SQL database roles.

The SQL database roles were permitted in the predicate function.

So the first two blocks demonstrate how users can be managed outside of SQL Server, and Windows groups mapped into SQL database roles are permitted in RLS predicates for minimum administrative overhead.

The following blocks (line 20 and below) are for various SQL users. See the file and comments.

### 5c-RLS-Remove.sql
This script simply removes the RLS implementation in _5a-RLS-Setup.sql_.

This is optional, and not needed if _2-Database.sql_ is run - dropping and re-creating the database in a clean baseline state.

