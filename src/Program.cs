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
    public class Program
    {
        const string XMLNS_PRODUCT_LVC = "http://www.kadaster.nl/schemas/bag-verstrekkingen/extract-producten-lvc/v20090901";
        const string XMLNS_BAG_LVC = "http://www.kadaster.nl/schemas/imbag/lvc/v20090901";
        const string XMLNS_BAG_TYPE = "http://www.kadaster.nl/schemas/imbag/imbag-types/v20090901";
        public static void Main(string[] args)
        {
            var start = DateTime.Now;
            Action<string> Log = s => Console.WriteLine($"{(DateTime.Now - start).TotalSeconds,9:###0.000}: {s}");

            Log($".NET Version {PlatformServices.Default.Application.RuntimeFramework.FullName}");

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
            var AanduidingRecordCorrectie = BAGlvc + "aanduidingRecordCorrectie";
            var Officieel = BAGlvc + "officieel";
            var InOnderzoek = BAGlvc + "inOnderzoek";
            var Bron = BAGlvc + "bron";
            var WoonplaatsStatus = BAGlvc + "woonplaatsStatus";

            Func<string, bool?> parseJN = s => s == "J" ? true :
                                               s == "N" ? false :
                                               (bool?)null;

            TimeZoneInfo CET;
            try
            {
                CET = TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");
            }
            catch
            {
                CET = TimeZoneInfo.FindSystemTimeZoneById("CET");
            }

            Func<string, DateTime?> parseDate = s =>
            {
                if (s == null)
                    return null;

                var d = DateTime.ParseExact(s, "yyyyMMddHHmmssff", CultureInfo.InvariantCulture);
                return TimeZoneInfo.ConvertTime(d, CET, TimeZoneInfo.Utc);
            };

            Log("Parsing XML");

            var parsed = XElement.Load("test.xml").Descendants(Woonplaats).Select(e =>
                {
                    /*
                    */
                    //Console.WriteLine(string.Join("\t", e.Elements().Select(x => x.Name)));
                    var tv = e.Element(Tijdvakgeldigheid);
                    return new
                    {
                        Identificatie = e.Element(Identificatie).Value,
                        AanduidingRecordInactief = parseJN(e.Element(AanduidingRecordInactief).Value).Value,
                        Officieel = parseJN(e.Element(Officieel).Value).Value,
                        InOnderzoek = parseJN(e.Element(InOnderzoek).Value).Value,
                        WoonplaatsStatus = e.Element(WoonplaatsStatus).Value,
                        AanduidingRecordCorrectie = int.Parse(e.Element(AanduidingRecordCorrectie).Value),
                        BegindatumTijdvakGeldigheid = parseDate(tv.Element(begindatumTijdvakGeldigheid)?.Value),
                        EinddatumTijdvakGeldigheid = parseDate(tv.Element(einddatumTijdvakGeldigheid)?.Value),
                        WoonplaatsNaam = e.Element(WoonplaatsNaam).Value,
                        WoonplaatsGeometrie = parseGML(e.Element(WoonplaatsGeometrie).Elements().Single()),
                    };
                });//.ToList();
            //Console.WriteLine(JsonConvert.SerializeObject(parsed, Formatting.Indented));

            Log("Connecting to database");
            using (var c = new Npgsql.NpgsqlConnection("Host=192.168.3.1;Username=test;Port=5434;Password=test"))
            {
                c.Open();
                using (var t = c.BeginTransaction())
                {
                    Log("TRUNCATE woonplaats");
                    using (var cmd = new NpgsqlCommand("TRUNCATE woonplaats", c))
                        cmd.ExecuteNonQuery();

                    Log("COPY woonplaats");

                    Copy(
                        c,
                        tablename: "woonplaats",
                        colnames: new[] {
                            "identificatie",
                            "aanduidingrecordinactief",
                            "woonplaatsnaam",
                            "begindatumtijdvakgeldigheid",
                            "einddatumtijdvakgeldigheid",
                            "officieel",
                            "inonderzoek",
                            "woonplaatsstatus",
                            "aanduidingrecordcorrectie",
                            "woonplaatsgeometrie",
                        },
                        data: parsed.Select(
                            p => new object[] {
                                p.Identificatie,
                                p.AanduidingRecordInactief,
                                p.WoonplaatsNaam,
                                p.BegindatumTijdvakGeldigheid,
                                p.EinddatumTijdvakGeldigheid,
                                p.Officieel,
                                p.InOnderzoek,
                                p.WoonplaatsStatus,
                                p.AanduidingRecordCorrectie,
                                p.WoonplaatsGeometrie,
                            }
                        )
                    );
                    Log("COMMIT");
                    t.Commit();
                }
            }
            Log("All done");
            Console.ReadKey();
        }

        private static void TestMultiGeometry(NpgsqlConnection c)
        {
            c.Open();
            using (var t = c.BeginTransaction())
            {
                using (var cmd = new NpgsqlCommand("CREATE TEMPORARY TABLE test (g geometry)", c))
                    cmd.ExecuteNonQuery();

                var qry = $"COPY test (g) FROM STDIN (FORMAT BINARY)";

                using (var writer = c.BeginBinaryImport(qry))
                {
                    var p1 = new Coordinate2D(0, 0);
                    var p2 = new Coordinate2D(0, 0);
                    var p3 = new Coordinate2D(0, 0);
                    var p = new PostgisPolygon(new[] { new[] { p1, p2, p3, p1 } });
                    var m = new PostgisGeometryCollection(new[] { p });
                    for (int i = 0; i < 1000; i++)
                        writer.WriteRow(m);
                }

                using (var cmd = new NpgsqlCommand("SELECT count(*) FROM test", c))
                    Console.WriteLine(cmd.ExecuteScalar());

                using (var cmd = new NpgsqlCommand("DROP TABLE test", c))
                    cmd.ExecuteNonQuery();

            }
            c.Close();
        }

        static XNamespace GML = "http://www.opengis.net/gml";
        static XName GMLMultiSurface = GML + "MultiSurface";
        static XName GMLsurfaceMember = GML + "surfaceMember";
        static XName GMLPolygon = GML + "Polygon";
        static XName GMLexterior = GML + "exterior";
        static XName GMLinterior = GML + "interior";
        static XName GMLLinearRing = GML + "LinearRing";
        static XName GMLposList = GML + "posList";

        private static PostgisGeometry parseGML(XElement gmlElement)
        {
            uint srid = 0;
            var srsName = gmlElement.Attribute("srsName")?.Value;

            if (srsName != null)
            {
                const string PREFIX = "urn:ogc:def:crs:EPSG::";
                if (srsName != null && !srsName.StartsWith(PREFIX))
                    throw new NotImplementedException($"Unknown srsName: {srsName}");

                srid = uint.Parse(srsName.Substring(PREFIX.Length));
            }

            if (gmlElement.Name == GMLPolygon)
            {
                var exterior = gmlElement.Elements(GMLexterior).Single();
                var interiors = gmlElement.Elements(GMLinterior);
                var iors = (new[] { exterior }).Union(interiors);
                return new PostgisPolygon(iors.Select(e => parseGMLRing(e.Elements(GMLLinearRing).Single()))) { SRID = srid };
            }
            else if (gmlElement.Name == GMLMultiSurface)
            {
                var inners = gmlElement.Elements(GMLsurfaceMember).Select(e => parseGML(e.Elements().Single())).ToArray();
                var all_poygons = inners.All(e => e is PostgisPolygon);
                if (all_poygons)
                    return new PostgisMultiPolygon(inners.Cast<PostgisPolygon>()) { SRID = srid };
                else
                    return new PostgisGeometryCollection(inners) { SRID = srid };
            }
            else
                throw new NotImplementedException($"No pg conversion for {gmlElement.Name}");

        }

        private static Coordinate2D[] parseGMLRing(XElement ringElement)
        {
            var poslist = ringElement.Elements(GMLposList).Single();
            if (poslist.Attribute("srsDimension").Value != "2")
                throw new NotImplementedException("taart");

            var numbers = poslist.Value.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var r = new Coordinate2D[numbers.Length / 2];
            for (int i = 0; i < r.Length; i++)
            {
                r[i] = new Coordinate2D(double.Parse(numbers[i * 2]), double.Parse(numbers[i * 2 + 1]));
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

        static NpgsqlDbType GetColumnNpgsqlDbType(NpgsqlDbColumn c)
        {
            return c.PostgresType.NpgsqlDbType ?? NpgsqlDbType.Text;
        }

        static void Copy(NpgsqlConnection conn, string tablename, string[] colnames, IEnumerable<object[]> data)
        {
            Func<string, string> Q = n => "\"" + n.Replace("\"", "\"\"") + "\"";
            var sql_tablename = Q(tablename);
            var sql_colnames = string.Join(", ", colnames.Select(n => Q(n)));
            var qry = $"COPY {sql_tablename} ({sql_colnames}) FROM STDIN (FORMAT BINARY)";
            var types = GetColumnTypes(conn, tablename, colnames).Select(s => GetColumnNpgsqlDbType(s)).ToArray();

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
