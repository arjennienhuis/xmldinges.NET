using System;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Extensions.PlatformAbstractions;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Data.Spatial.DbGeography;

namespace ConsoleApplication
{
    public class Program
    {
        const string XMLNS_PRODUCT_LVC = "http://www.kadaster.nl/schemas/bag-verstrekkingen/extract-producten-lvc/v20090901";
        const string XMLNS_BAG_LVC = "http://www.kadaster.nl/schemas/imbag/lvc/v20090901";
        const string XMLNS_BAG_TYPE = "http://www.kadaster.nl/schemas/imbag/imbag-types/v20090901";
        public static void Main(string[] args)
        {
            var start = DateTime.Now;
            Action<string> Log = s => Console.WriteLine($"{(DateTime.Now - start).TotalSeconds,9:###0.000}: {s}");

            Log($".NET Version {PlatformServices.Default.Application.RuntimeFramework.Version}");

            XNamespace BAGlvc = XMLNS_BAG_LVC;
            XNamespace BAGtype = XMLNS_BAG_TYPE;
            var Woonplaats = BAGlvc + "Woonplaats";
            var Identificatie = BAGlvc + "identificatie";
            var AanduidingRecordInactief = BAGlvc + "aanduidingRecordInactief";

            var Tijdvakgeldigheid = BAGlvc + "tijdvakgeldigheid";
            var begindatumTijdvakGeldigheid = BAGtype + "begindatumTijdvakGeldigheid";
            var einddatumTijdvakGeldigheid = BAGtype + "einddatumTijdvakGeldigheid";
            var WoonplaatsNaam = BAGlvc + "woonplaatsNaam";
            var WoonplaatsGeometrie = BAGlvc + "woonplaatsGeometrie";

            Func<string, bool?> parseJN = s => s == "J" ? true :
                                               s == "N" ? false :
                                               (bool?)null;

            Log("Parsing XML");

            var parsed = XElement.Load("test.xml").Descendants(Woonplaats).Select(e =>
                {
                    /*
                        BAGlvc + "aanduidingRecordCorrectie"
                        BAGlvc + "officieel"
                        BAGlvc + "inOnderzoek"
                        BAGlvc + "bron"
                        BAGlvc + "woonplaatsStatus"
                    */
                    //Console.WriteLine(string.Join("\t", e.Elements().Select(x => x.Name)));
                    var tv = e.Element(Tijdvakgeldigheid);
                    return new
                    {
                        Identificatie = e.Element(Identificatie).Value,
                        AanduidingRecordInactief = parseJN(e.Element(AanduidingRecordInactief).Value).Value,
                        BegindatumTijdvakGeldigheid = tv.Element(begindatumTijdvakGeldigheid)?.Value,
                        EinddatumTijdvakGeldigheid = tv.Element(einddatumTijdvakGeldigheid)?.Value,
                        WoonplaatsNaam = e.Element(WoonplaatsNaam).Value,
                        WoonplaatsGeometrie = e.Element(WoonplaatsGeometrie).Elements().Single().ToString(),
                    };
                });//.ToList();
            //Console.WriteLine(JsonConvert.SerializeObject(parsed, Formatting.Indented));

            Log("Connecting to database");
            using (var c = new Npgsql.NpgsqlConnection("Host=localhost;Username=test;Port=5434;Password=test"))
            {
                c.Open();
                using (var t = c.BeginTransaction())
                {
                    GetColumnTypes(c, "woonplaats", new[] { "identificatie", "aanduidingrecordinactief", "woonplaatsnaam", "woonplaatsgeometrie" });
                    Log("TRUNCATE woonplaats");
                    using (var cmd = new NpgsqlCommand("TRUNCATE woonplaats", c))
                        cmd.ExecuteNonQuery();

                    Log("COPY woonplaats");

                    Copy(
                        c,
                        "woonplaats", 
                        new[] { "identificatie", "aanduidingrecordinactief", "woonplaatsnaam", "woonplaatsgeometrie" }, 
                        parsed.Select(p => new object[] { p.Identificatie, p.AanduidingRecordInactief, p.WoonplaatsNaam, p.WoonplaatsGeometrie })
                    );

                    Log("COMMIT");
                    t.Commit();
                }
            }
            Log("All done");
            // Console.ReadKey();
        }

        static List<Type> GetColumnTypes(NpgsqlConnection conn, string tablename, IEnumerable<string> colnames)
        {
            Func<string, string> Q = n => "\"" + n.Replace("\"", "\"\"") + "\"";
            var sql_tablename = Q(tablename);
            var sql_colnames = string.Join(", ", colnames.Select(n => Q(n)));
            var qry = $"SELECT {sql_colnames} FROM {sql_tablename} LIMIT 0";
            using (var cmd = new NpgsqlCommand(qry, conn))
            using (var r = cmd.ExecuteReader(System.Data.CommandBehavior.SchemaOnly))
                return r.GetColumnSchema().Select(s => s.DataType).ToList();
        }

        static Dictionary<Type, NpgsqlDbType> TypeTypeMap = new Dictionary<Type, NpgsqlDbType>
        {
            [typeof(Int64)] = NpgsqlDbType.Bigint,
            [typeof(Int32)] = NpgsqlDbType.Integer,
            [typeof(Int16)] = NpgsqlDbType.Smallint,
            [typeof(string)] = NpgsqlDbType.Text,
            [typeof(bool)] = NpgsqlDbType.Boolean,
            [typeof(PostgisGeometry)] = NpgsqlDbType.Geometry,
        };

        static NpgsqlDbType GetTypeType(Type type)
        {
            NpgsqlDbType result;
            if (TypeTypeMap.TryGetValue(type, out result))
                return result;
            else
                throw new NotImplementedException($"No pg conversion for {type}");
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
