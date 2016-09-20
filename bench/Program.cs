using System;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Extensions.PlatformAbstractions;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using Npgsql.Schema;
using System.Collections.Generic;
using System.Globalization;

namespace ConsoleApplication
{
    public class Program2
    {
        public static void Main(string[] args)
        {
            var start = DateTime.Now;
            var last = start;
            Action<string> Log = s => {
                var now = DateTime.Now;
                Console.WriteLine($"{(now - start).TotalSeconds,9:###0.000} {(now - last).TotalSeconds,9:###0.000}: {s}");
                last = now;
            };

            Log($".NET Version {PlatformServices.Default.Application.RuntimeFramework.FullName}");

            Log("Gen data1");
            var data1 = GenData(count: 100000, size: 10);

            Log("Gen data2");
            var data2 = GenData(count: 1, size: 1000000);

            Log("Connecting to database");
            using (var c = new Npgsql.NpgsqlConnection("Host=192.168.3.1;Username=test;Port=5434;Password=test"))
            {
                c.Open();
                using (var t = c.BeginTransaction())
                {
                    Log("CREATE TABLE");
                    using (var cmd = new NpgsqlCommand("CREATE TEMPORARY TABLE test (t hstore)", c))
                        cmd.ExecuteNonQuery();

                    Log("COPY data1");
                    Copy(c, "test", new[] {"t"}, data1);
                    Log("COPY done.");

                    Log("COPY data2");
                    Copy(c, "test", new[] {"t"}, data2);
                    Log("COPY done.");

                    var rows = 0;
                    var l = 0;

                    Log("SELECT");
                    using (var cmd = new NpgsqlCommand("SELECT * FROM test", c))
                    using (var r = cmd.ExecuteReader())
                        while (r.Read())
                        {
                            rows++;
                            l += ((IDictionary<string, string>)r[0]).Count;
                        }
                    Log($"SELECT done. {rows} rows, {l} total length.");

                    Log("ROLLBACK");
                    t.Rollback();
                }
            }
            Log("All done");
            Console.ReadKey();
        }

        private static List<Dictionary<string, string>[]> GenData(int size, int count)
        {
            var r = new List<Dictionary<string, string>[]>(count);
            for (int i = 0; i < count; i++)
            {
                var d = new Dictionary<string, string>(size);
                for (int k = 0; k < size; k++)
                    d.Add($"K{i}-{k}", $"V{k}");
                r.Add(new[] {d});
            }
            return r;
        }

        static List<NpgsqlDbColumn> GetColumnTypes(NpgsqlConnection conn, string tablename, IEnumerable<string> colnames)
        {
            Func<string, string> Q = n => "\"" + n.Replace("\"", "\"\"") + "\"";
            var sql_tablename = Q(tablename);
            var sql_colnames = string.Join(", ", colnames.Select(n => Q(n)));
            var qry = $"SELECT {sql_colnames} FROM {sql_tablename} LIMIT 0";
            using (var cmd = new NpgsqlCommand(qry, conn))
            using (var r = cmd.ExecuteReader(System.Data.CommandBehavior.SchemaOnly))
                return r.GetColumnSchema().ToList();
        }

        static Dictionary<uint, NpgsqlDbType> TypeOIDTypeMap = new Dictionary<uint, NpgsqlDbType>
        {
            [1082] = NpgsqlDbType.Date,
            [1184] = NpgsqlDbType.TimestampTZ,
            [1114] = NpgsqlDbType.Timestamp,
        };

        static Dictionary<Type, NpgsqlDbType> TypeTypeMap = new Dictionary<Type, NpgsqlDbType>
        {
            [typeof(Int64)] = NpgsqlDbType.Bigint,
            [typeof(Int32)] = NpgsqlDbType.Integer,
            [typeof(Int16)] = NpgsqlDbType.Smallint,
            [typeof(string)] = NpgsqlDbType.Text,
            [typeof(bool)] = NpgsqlDbType.Boolean,
            [typeof(PostgisGeometry)] = NpgsqlDbType.Geometry,
            [typeof(Dictionary<string, string>)] = NpgsqlDbType.Hstore,
        };

        static NpgsqlDbType GetTypeType(NpgsqlDbColumn c)
        {
            Console.WriteLine($"TYPE: {c.DataType} {c.DataTypeName} {c.TypeOID}");
            NpgsqlDbType result;
            if (
                TypeOIDTypeMap.TryGetValue(c.TypeOID, out result)
                || 
                TypeTypeMap.TryGetValue(c.DataType, out result)
            )
                return result;
            else
                throw new NotImplementedException($"No pg conversion for {c.DataType}");
        }

        static void Copy(NpgsqlConnection conn, string tablename, string[] colnames, IEnumerable<object[]> data)
        {
            Func<string, string> Q = n => "\"" + n.Replace("\"", "\"\"") + "\"";
            var sql_tablename = Q(tablename);
            var sql_colnames = string.Join(", ", colnames.Select(n => Q(n)));
            var qry = $"COPY {sql_tablename} ({sql_colnames}) FROM STDIN (FORMAT BINARY)";
            var types = GetColumnTypes(conn, tablename, colnames).Select(s => GetTypeType(s)).ToArray();

            using (var writer = conn.BeginBinaryImport(qry))
            {
                int rownum = 0;
                foreach (var row in data)
                {
                    if (row.Length != types.Length)
                        throw new ArgumentException($"Row {rownum} has wrong lenth of {row.Length}");
                    int colnum = 0;
                    try
                    {
                        writer.StartRow();
                        for (colnum = 0; colnum < types.Length; colnum++)
                            writer.Write(row[colnum], types[colnum]);
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"Error importing row {rownum} column {colnum} ({Q(colnames[colnum])})", e);
                    }
                    rownum++;
                }
            }
        }
    }
}
