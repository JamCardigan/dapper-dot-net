using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper.Testable.IntergrationTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Dapper.Testable.IntergrationTests
{
    [TestClass]
    public class TestWrapperStoredProcedures
    {
        private readonly SqlDataAndSchema _dataAndSchema;

        public TestWrapperStoredProcedures()
        {
            _dataAndSchema = new SqlDataAndSchema();
        }

        [ClassInitialize]
        public static void InitClass(TestContext context)
        {
            SqlDataAndSchema.DatabaseCreate();
        }

        [ClassCleanup]
        public static void CleanupClass()
        {
            SqlDataAndSchema.DatabaseDrop();
        }

        [TestInitialize]
        public void Init()
        {
            _dataAndSchema.SchemaSetup();
            _dataAndSchema.CreateTestData(thisMany: 100);
            _dataAndSchema.CreateTestData(_dataAndSchema.Value);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _dataAndSchema.SchemaTearDown();
        }
        
        [TestMethod]
        public void QueryTyped_WithDynamicParams_Executes()
        {
            EntityBar result;
            using (var c = _dataAndSchema.GetConnection())
            {
                var dapper = new SqlMapperWrapper(c);

                c.Open();

                var args = new DynamicParameters(new { val = _dataAndSchema.Value.ToString() });

                result = dapper.Query<EntityBar>(
                    SqlDataAndSchema.SprocNameDapperSingleInParam,
                    args,
                    commandType: CommandType.StoredProcedure).SingleOrDefault();
            }

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void QueryDynamic_WithDynamicParams_Executes()
        {
            dynamic result;
            using (var c = _dataAndSchema.GetConnection())
            {
                c.Open();

                var args = new DynamicParameters(new { val = _dataAndSchema.Value.ToString() });

                result = c.Query<dynamic>(
                    SqlDataAndSchema.SprocNameDapperSingleInParam,
                    args,
                    commandType: CommandType.StoredProcedure).SingleOrDefault();
            }

            Assert.IsNotNull(result);
        }
        
        [TestMethod]
        public void QueryTyped_WithAnonymousParams_Executes()
        {
            EntityBar result;
            using (var c = _dataAndSchema.GetConnection())
            {
                var dapper = new SqlMapperWrapper(c);

                c.Open();

                var args = new { val = _dataAndSchema.Value.ToString() };

                result = dapper.Query<EntityBar>(
                    SqlDataAndSchema.SprocNameDapperSingleInParam,
                    args,
                    commandType: CommandType.StoredProcedure).SingleOrDefault();
            }

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void QueryDynamic_WithAnonymousParams_Executes()
        {
            dynamic result;
            using (var c = _dataAndSchema.GetConnection())
            {
                c.Open();

                var args = new { val = _dataAndSchema.Value.ToString() };

                result = c.Query<dynamic>(
                    SqlDataAndSchema.SprocNameDapperSingleInParam,
                    args,
                    commandType: CommandType.StoredProcedure).SingleOrDefault();
            }

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void QueryMultiple_WithDynamicParams_Executes()
        {
            List<EntityFoo> secondResult;
            List<EntityBar> result;
            using (var c = _dataAndSchema.GetConnection())
            {
                c.Open();

                var args = new { val = _dataAndSchema.Value.ToString() };

                var reader = new SqlMapperWrapper(c).QueryMultiple(
                    SqlDataAndSchema.SprocNameDapperSingleInMultipleResultSets,
                    args,
                    commandType: CommandType.StoredProcedure);

                secondResult = reader.Read<EntityFoo>().ToList();
                result = reader.Read<EntityBar>().ToList();

            }

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Any());
            Assert.IsNotNull(secondResult);
            Assert.IsTrue(secondResult.Count() > 1);
        }

        [TestMethod]
        public void QueryTyped_WithAnonymousParamsAndOutParam_Executes()
        {
            using (var c = _dataAndSchema.GetConnection())
            {
                c.Open();

                var w = new SqlMapperWrapper(c);

                IParametersFactory fact = new ParametersFactory();

                var args = fact.CreateInstance(new {Val = _dataAndSchema.Value.ToString()});
                args.Add("@affected", null, DbType.Int32, ParameterDirection.InputOutput);

                var result = w.Query<EntityBar>(
                    SqlDataAndSchema.SprocNameDapperSingleInSingleOutParam,
                    args,
                    commandType: CommandType.StoredProcedure).ToList();

                var outparam = args.Get<int>("@affected");

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count() == 1);
                Assert.IsTrue(outparam == 1);
            }
        }
    }
}
