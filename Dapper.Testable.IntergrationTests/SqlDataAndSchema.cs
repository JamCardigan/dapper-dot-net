using System;
using System.Data.SqlClient;

namespace Dapper.Testable.IntergrationTests
{
    public class SqlDataAndSchema
    {
        #region const
        private const string DatabaseName = "2CC5C5C8-3CBD-486C-B303-3A8FC4EB6595";
        public const string SprocNameDapperSingleInParam = "dapperSingleInParam";
        public const string SprocNameDapperSingleInSingleOutParam = "dapperSingleInSingleOutParam";
        public const string SprocNameDapperSingleInMultipleResultSets = "dapperSingleInMultipleSetsSelect";
        public readonly Guid Value = Guid.NewGuid();
        #endregion

        #region fields
        private static bool _databaseCreated;
        #endregion

        #region methods
        public SqlConnection GetConnection()
        {
            return new SqlConnection(string.Format(@"Data Source=.;Initial Catalog={0};Integrated Security=SSPI;", DatabaseName));
        }

        private static SqlConnection GetConnectionToMaster()
        {
            return new SqlConnection(string.Format(@"Data Source=.;Initial Catalog=master;Integrated Security=SSPI;"));
        }

        public void CreateTestData(Guid id = default(Guid), int thisMany = 1)
        {
            using (var c = GetConnection())
            {
                c.Open();
                for (var i = 0; i < thisMany; i++)
                {
                    c.Execute("insert into dbo.dapper (val) values(@val)",
                        new { val = id == default(Guid) ? Guid.NewGuid().ToString() : id.ToString() });
                }
            }
        }

        public static void DatabaseCreate()
        {
            if (_databaseCreated)
                return;

            using (var c = GetConnectionToMaster())
            {
                c.Execute(string.Format(@"
CREATE DATABASE [{0}] ON  PRIMARY 
( NAME = N'{0}', FILENAME = N'{1}\{0}.mdf')
 LOG ON 
( NAME = N'{0}_log', FILENAME = N'{1}\{0}_log.ldf')",
                    DatabaseName,
                    System.IO.Path.GetTempPath()));

            }
            _databaseCreated = true;
        }

        public static void DatabaseDrop()
        {
            using (var c = GetConnectionToMaster())
            {
                c.Execute(string.Format(@"alter database [{0}] set single_user with rollback immediate", DatabaseName));
                c.Execute(string.Format(@"drop database [{0}]", DatabaseName));
            }
        }

        public void SchemaSetup()
        {
           using (var c = GetConnection())
            {
                c.Open();
                var t = c.BeginTransaction();
                try
                {
                    //insert the table required for the tests
                    c.Execute(string.Format(@"
                        create table [{0}].[dbo].[dapper](
	                        [id] [int] IDENTITY(1,1) NOT NULL,
	                        [val] [varchar](255) NOT NULL,
	                        [ts] [timestamp] NOT NULL
                        ) ON [PRIMARY]", DatabaseName)
                        , transaction: t);

                    //insert the sproc 
                    c.Execute(String.Format(@"
                        create procedure [dbo].[{0}]
	                        @val varchar(255)
                        as
                        begin
	                        declare @r table(id int, ts binary(8))

	                        insert 
		                        dbo.dapper (val) 
	                        output
		                        inserted.Id,
		                        inserted.ts
	                        into
		                        @r
	                        values  
		                        (@val)
		
	                        select 
		                        * 
	                        from
		                        @r 
                        end", SprocNameDapperSingleInParam), transaction: t);

                    c.Execute(String.Format(@"
                        create procedure [dbo].[{0}]
	                        @val varchar(255)
                            ,@affected int output
                        as
                        begin
	                        declare @r table(id int, ts binary(8))

	                        insert 
		                        dbo.dapper (val) 
	                        output
		                        inserted.Id,
		                        inserted.ts
	                        into
		                        @r
	                        values 
		                        (@val)
                        
                            set @affected = @@rowcount		

	                        select 
		                        * 
	                        from
		                        @r 
                        end", SprocNameDapperSingleInSingleOutParam), transaction: t);

                    c.Execute(string.Format(@"
                        create procedure [dbo].[{0}]
                            @val varchar(255)
                        as
                        begin
                            select id as [key], val as value
                            from dbo.dapper

                            select id, val, ts
                            from dbo.dapper
                            where val = @val
                        end", SprocNameDapperSingleInMultipleResultSets), transaction: t);
                }
                catch
                {
                    t.Rollback();
                    throw;
                }
                t.Commit();
            }
        }

        public void SchemaTearDown()
        {
            using (var c = GetConnection())
            {
                c.Execute("drop table dapper");
                c.Execute(string.Format("drop procedure {0}", SprocNameDapperSingleInParam));
                c.Execute(string.Format("drop procedure {0}",
                    SprocNameDapperSingleInSingleOutParam));
                c.Execute(string.Format("drop procedure {0}",
                    SprocNameDapperSingleInMultipleResultSets));
            }
        }
        #endregion
    }
}