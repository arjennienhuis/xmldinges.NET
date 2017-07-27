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
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using System.Xml;

namespace ConsoleApplication
{
    public class Program
    {
        const string XMLNS_PRODUCT_LVC = "http://www.kadaster.nl/schemas/bag-verstrekkingen/extract-producten-lvc/v20090901";
        const string XMLNS_BAG_LVC = "http://www.kadaster.nl/schemas/imbag/lvc/v20090901";
        const string XMLNS_BAG_TYPE = "http://www.kadaster.nl/schemas/imbag/imbag-types/v20090901";

        public static void Main(string[] args)
        {
            var cla = new CommandLineApplication();
            var import_types_arg = cla.Option(
                "--import-types <type>",
                $"Type of file to import: {string.Join(", ", ParamsDict.Keys)}",
                CommandOptionType.MultipleValue
            );
            var max_files_per_type_arg = cla.Option(
                "--n-files <n>",
                "Number of files to import per type. The table will be truncated.",
                CommandOptionType.SingleValue
            );
            var import_dir_arg = cla.Option(
                "--import-dir <path>",
                "Here are the zip files",
                CommandOptionType.SingleValue
            );
            cla.HelpOption("-? | -h | --help");
            cla.OnExecute(() => {
                int? max_files_per_type = max_files_per_type_arg.HasValue() ? int.Parse(max_files_per_type_arg.Value()) : (int?)null;
                Main2(import_types_arg.Values, max_files_per_type, import_dir: import_dir_arg.Value());
                return 0;
            });
            cla.Execute(args);
        }

        public static void Main2(List<string> import_types, int? max_files_per_type, string import_dir)
        {
            var start = DateTime.Now;
            Action<string> Log = s => Console.WriteLine($"{(DateTime.Now - start).TotalSeconds,9:###0.000}: {s}");

            xmldinges.NET.ProcessDir.OpenAll(
                import_dir ?? @"C:\Users\Arjen\BAGDATA\inspireadressen",
                log: Log,
                import_types: import_types,
                max_files_per_type: max_files_per_type
            );
        }

        private static TimeZoneInfo GetCET()
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");
            }
            catch
            {
                return TimeZoneInfo.FindSystemTimeZoneById("CET");
            }

        }

        public static readonly TimeZoneInfo CET = GetCET();

        static bool? parseJN(string s) => s == "J" ? true :
                                          s == "N" ? false :
                                          (bool?)null;

        static DateTime? parseDate(string s)
        {
            if (s == null)
                return null;

            var d = DateTime.ParseExact(s, "yyyyMMddHHmmssff", CultureInfo.InvariantCulture);
            return TimeZoneInfo.ConvertTime(d, CET, TimeZoneInfo.Utc);
        }

        static readonly XNamespace NS_BAG_LVC = XMLNS_BAG_LVC;
        static readonly XNamespace NS_BAG_TYPE = XMLNS_BAG_TYPE;
        static readonly XName BAG_LVC_Nummeraanduiding = NS_BAG_LVC + "Nummeraanduiding";
        static readonly XName BAG_LVC_OpenbareRuimte = NS_BAG_LVC + "OpenbareRuimte";
        static readonly XName BAG_LVC_Pand = NS_BAG_LVC + "Pand";
        static readonly XName BAG_LVC_Woonplaats = NS_BAG_LVC + "Woonplaats";
        static readonly XName BAG_LVC_Verblijfsobject = NS_BAG_LVC + "Verblijfsobject";
        static readonly XName BAG_LVC_Standplaats = NS_BAG_LVC + "Standplaats";
        static readonly XName BAG_LVC_Ligplaats = NS_BAG_LVC + "Ligplaats";
        static readonly XName BAG_LVC_Identificatie = NS_BAG_LVC + "identificatie";
        static readonly XName BAG_LVC_AanduidingRecordInactief = NS_BAG_LVC + "aanduidingRecordInactief";
        static readonly XName BAG_LVC_Tijdvakgeldigheid = NS_BAG_LVC + "tijdvakgeldigheid";
        static readonly XName BAGyypebegindatumTijdvakGeldigheid = NS_BAG_TYPE + "begindatumTijdvakGeldigheid";
        static readonly XName BAGyypeeinddatumTijdvakGeldigheid = NS_BAG_TYPE + "einddatumTijdvakGeldigheid";
        static readonly XName BAG_LVC_WoonplaatsNaam = NS_BAG_LVC + "woonplaatsNaam";
        static readonly XName BAG_LVC_WoonplaatsGeometrie = NS_BAG_LVC + "woonplaatsGeometrie";
        static readonly XName BAG_LVC_verblijfsobjectGeometrie = NS_BAG_LVC + "verblijfsobjectGeometrie";
        static readonly XName BAG_LVC_AanduidingRecordCorrectie = NS_BAG_LVC + "aanduidingRecordCorrectie";
        static readonly XName BAG_LVC_Officieel = NS_BAG_LVC + "officieel";
        static readonly XName BAG_LVC_InOnderzoek = NS_BAG_LVC + "inOnderzoek";
        static readonly XName BAG_LVC_Bron = NS_BAG_LVC + "bron";
        static readonly XName BAG_LVC_WoonplaatsStatus = NS_BAG_LVC + "woonplaatsStatus";
        static readonly XName BAG_LVC_verblijfsobjectStatus = NS_BAG_LVC + "verblijfsobjectStatus";
        static readonly XName BAG_LVC_gebruiksdoelVerblijfsobject = NS_BAG_LVC + "gebruiksdoelVerblijfsobject";
        static readonly XName BAG_LVC_gerelateerdPand = NS_BAG_LVC + "gerelateerdPand";
        static readonly XName BAG_LVC_gerelateerdeAdressen = NS_BAG_LVC + "gerelateerdeAdressen";
        static readonly XName BAG_LVC_hoofdadres = NS_BAG_LVC + "hoofdadres";
        static readonly XName BAG_LVC_nevenadres = NS_BAG_LVC + "nevenadres";
        static readonly XName BAG_LVC_oppervlakteVerblijfsobject = NS_BAG_LVC + "oppervlakteVerblijfsobject";
        static readonly XName BAG_LVC_huisnummer = NS_BAG_LVC + "huisnummer";
        static readonly XName BAG_LVC_huisletter = NS_BAG_LVC + "huisletter";
        static readonly XName BAG_LVC_huisnummertoevoeging = NS_BAG_LVC + "huisnummertoevoeging";
        static readonly XName BAG_LVC_postcode = NS_BAG_LVC + "postcode";
        static readonly XName BAG_LVC_GerelateerdeWoonplaats = NS_BAG_LVC + "GerelateerdeWoonplaats";
        static readonly XName BAG_LVC_gerelateerdeWoonplaats = NS_BAG_LVC + "gerelateerdeWoonplaats";
        static readonly XName BAG_LVC_gerelateerdeOpenbareRuimte = NS_BAG_LVC + "gerelateerdeOpenbareRuimte";
        static readonly XName BAG_LVC_typeAdresseerbaarObject = NS_BAG_LVC + "typeAdresseerbaarObject";
        static readonly XName BAG_LVC_nummeraanduidingStatus = NS_BAG_LVC + "nummeraanduidingStatus";
        static readonly XName BAG_LVC_pandGeometrie = NS_BAG_LVC + "pandGeometrie";
        static readonly XName BAG_LVC_bouwjaar = NS_BAG_LVC + "bouwjaar";
        static readonly XName BAG_LVC_pandstatus = NS_BAG_LVC + "pandstatus";
        static readonly XName BAG_LVC_openbareRuimteNaam = NS_BAG_LVC + "openbareRuimteNaam";
        static readonly XName BAG_LVC_openbareRuimteType = NS_BAG_LVC + "openbareRuimteType";
        static readonly XName BAG_LVC_openbareruimteStatus = NS_BAG_LVC + "openbareruimteStatus";
        static readonly XName BAG_LVC_standplaatsStatus = NS_BAG_LVC + "standplaatsStatus";
        static readonly XName BAG_LVC_standplaatsGeometrie = NS_BAG_LVC + "standplaatsGeometrie";
        static readonly XName BAG_LVC_ligplaatsStatus = NS_BAG_LVC + "ligplaatsStatus";
        static readonly XName BAG_LVC_ligplaatsGeometrie = NS_BAG_LVC + "ligplaatsGeometrie";
        static readonly XNamespace NS_brk_krt = "https://www.kadaster.nl/schemas/brkkadastralekaart/v20160101";
        static readonly XName brk_krt_Perceel = NS_brk_krt + "Perceel";
        static readonly XName brk_krt_identificatie = NS_brk_krt + "identificatie";
        static readonly XName brk_krt_begrenzingPerceel = NS_brk_krt + "begrenzingPerceel";
        static readonly XName brk_krt_NEN3610ID = NS_brk_krt + "NEN3610ID";
        static readonly XName brk_krt_lokaalID = NS_brk_krt + "lokaalID";

        struct ImportParams
        {
            public string[] column_names;
            public string table_name;
            public XName element_name;
            public Func<XElement, object[]> parse;
        }

        static Dictionary<string, ImportParams> ParamsDict = new Dictionary<string, ImportParams>()
        {
            ["OPR"] = new ImportParams
            {
                /*
                    Identificatiecode openbare ruimte
                    aanduidingRecordInactief
                    aanduidingRecordCorrectie
                    Naam
                    Indicatie geconstateerde openbare ruimte
                    Datum begin geldigheid gegevens openbare ruimte
                    Datum einde geldigheid gegevens openbare ruimte
                    Aanduiding gegevens openbare ruimte in onderzoek
                    Identificatiecode bijbehorende woonplaats
                    Type openbare ruimte
                    Status openbare ruimte
                */
                table_name = "openbare_ruimte",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "naam",
                    "officieel",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                    "inonderzoek",
                    "woonplaats_id",
                    "type",
                    "status",
                },
                element_name = BAG_LVC_OpenbareRuimte,
                parse = e =>
                {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                               // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,     // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),        // correctienummer
                        e.Element(BAG_LVC_openbareRuimteNaam).Value,                          // naam
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                    // officieel
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),      // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),      // einddatumtijdvakgeldigheid
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                  // inonderzoek
                        e.Element(BAG_LVC_gerelateerdeWoonplaats)
                            .Element(BAG_LVC_Identificatie).Value,                            // woonplaats_id
                        e.Element(BAG_LVC_openbareRuimteType).Value,                          // type
                        e.Element(BAG_LVC_openbareruimteStatus).Value,                        // status
                   };
                },
            },
            ["PND"] = new ImportParams
            {
                /*
                    55.01 Pandidentificatie
                    55.02 Indicatie geconstateerd pand
                    55.20 Pandgeometrie
                    55.30 Oorspronkelijk bouwjaar pand
                    55.31 Pandstatus
                    55.91 Datum begin geldigheid pandgegevens
                    55.92 Datum einde geldigheid pandgegevens
                    55.93 Aanduiding pandgegevens in onderzoek
                    55.97 Documentdatum mutatie pand
                    55.98 Documentnummer mutatie pand
                */
                table_name = "pand",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "officieel",
                    "geometrie",
                    "bouwjaar",
                    "status",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                    "inonderzoek",
                },
                element_name = BAG_LVC_Pand,
                parse = e =>
                {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                               // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,     // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),        // correctienummer
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                    // officieel
                        parseGML(e.Element(BAG_LVC_pandGeometrie).Elements().Single()),       // geometrie
                        int.Parse(e.Element(BAG_LVC_bouwjaar).Value),                         // bouwjaar
                        e.Element(BAG_LVC_pandstatus).Value,                                  // status
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),     // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),      // einddatumtijdvakgeldigheid
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                  // inonderzoek
                   };
                },
            },
            ["NUM"] = new ImportParams
            {
                /*
                    11.02 Identificatiecode nummeraanduiding
                    11.20 Huisnummer
                    11.21 Indicatie geconstateerde nummeraanduiding
                    11.30 Huisletter
                    11.40 Huisnummertoevoeging
                    11.60 Postcode
                    11.61 Identificatiecode bijbehorende woonplaats
                    11.62 Datum begin geldigheid nummeraanduidinggegevens
                    11.63 Datum einde geldigheid nummeraanduidinggegevens
                    11.64 Aanduiding nummeraanduidinggegevens in onderzoek
                    11.65 Identificatiecode bijbehorende openbare ruimte
                    11.66 Type adresseerbaar object
                    11.67 Documentdatum mutatie nummeraanduiding
                    11.68 Documentnummer mutatie nummeraanduiding
                    11.69 Nummeraanduidingstatus
                */
                table_name = "nummeraanduiding",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "officieel",
                    "huisnummer",
                    "huisletter",
                    "toevoeging",
                    "postcode",
                    "woonplaats_id",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                    "inonderzoek",
                    "openbare_ruimte_id",
                    "type",
                    "status",
                },
                element_name = BAG_LVC_Nummeraanduiding,
                parse = e =>
                {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                               // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,     // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),        // correctienummer
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                    // officieel
                        e.Element(BAG_LVC_huisnummer).Value,                                  // huisnummer
                        e.Element(BAG_LVC_huisletter)?.Value,                                 // huisletter
                        e.Element(BAG_LVC_huisnummertoevoeging)?.Value,                       // toevoeging
                        e.Element(BAG_LVC_postcode)?.Value,                                   // postcode
                        e.Element(BAG_LVC_GerelateerdeWoonplaats)
                            ?.Element(BAG_LVC_Identificatie).Value,                           // woonplaats_id
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),     // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),      // einddatumtijdvakgeldigheid
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                  // inonderzoek
                        e.Element(BAG_LVC_gerelateerdeOpenbareRuimte)
                         .Element(BAG_LVC_Identificatie).Value,                               // openbare_ruimte_id
                        e.Element(BAG_LVC_typeAdresseerbaarObject).Value,                     // type
                        e.Element(BAG_LVC_nummeraanduidingStatus).Value,                      // status
                   };
                },
            },
            ["WPL"] = new ImportParams
            {
                /*
                    11.03 Woonplaatsidentificatie
                    11.70 Woonplaatsnaam
                    11.71 Woonplaatsgeometrie
                    11.72 Indicatie geconstateerde woonplaats
                    11.73 Datum begin geldigheid woonplaatsgegevens
                    11.74 Datum einde geldigheid woonplaatsgegevens
                    11.75 Aanduiding woonplaatsgegevens in onderzoek
                 !! 11.77 Documentdatum mutatie woonplaats
                 !! 11.78 Documentnummer mutatie woonplaats
                    11.79 Woonplaatsstatus
                */
                table_name = "woonplaats",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "woonplaatsnaam",
                    "woonplaatsgeometrie",
                    "officieel",
                    "inonderzoek",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                    "woonplaatsstatus",
                },
                element_name = BAG_LVC_Woonplaats,
                parse = e =>
                {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                               // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,     // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),        // correctienummer
                        e.Element(BAG_LVC_WoonplaatsNaam).Value,                              // woonplaatsnaam
                        parseGML(e.Element(BAG_LVC_WoonplaatsGeometrie).Elements().Single()), // woonplaatsgeometrie
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                    // officieel
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                  // inonderzoek
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),     // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),      // einddatumtijdvakgeldigheid
                        e.Element(BAG_LVC_WoonplaatsStatus).Value,                            // woonplaatsstatus
                   };
                },
            },
            ["VBO"] = new ImportParams
            {
                table_name = "verblijfsobject",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "officieel",
                    "inonderzoek",
                    "verblijfsobjectstatus",
                    "gebruiksdoelen",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                    "verblijfsobjectgeometrie",
                    "pand_ids",
                    "hoofdadres_id",
                    "nevenadres_ids",
                    "oppervlakte",
                },
                element_name = BAG_LVC_Verblijfsobject,
                parse = e =>
                {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    var rel_ads = e.Element(BAG_LVC_gerelateerdeAdressen);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                                    // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,          // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),             // correctienummer
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                         // officieel
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                       // inonderzoek
                        e.Element(BAG_LVC_verblijfsobjectStatus).Value,                            // verblijfsobjectstatus
                        e.Elements(BAG_LVC_gebruiksdoelVerblijfsobject)
                         .Select(g => g.Value)
                         .ToList(),                                                                // gebruiksdoelen
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),           // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),           // einddatumtijdvakgeldigheid
                        parseGML(e.Element(BAG_LVC_verblijfsobjectGeometrie).Elements().Single()), // verblijfsobjectgeometrie
                        e.Elements(BAG_LVC_gerelateerdPand).Select(
                            p => p.Element(BAG_LVC_Identificatie).Value
                        ).ToList(),                                                                // pand_ids
                        rel_ads.Element(BAG_LVC_hoofdadres).Element(BAG_LVC_Identificatie).Value,  // hoofdadres_id
                        rel_ads.Elements(BAG_LVC_nevenadres).Select(
                            na => na.Element(BAG_LVC_Identificatie).Value
                        ).ToList(),                                                                // nevenadres_ids
                        e.Element(BAG_LVC_oppervlakteVerblijfsobject).Value,                       // opervlakte
                   };
                },
            },
            ["STA"] = new ImportParams(){
                /*
                    57.01 Standplaatsidentificatie
                    57.02 Indicatie geconstateerde standplaats
                    57.03 Standplaatsstatus
                    57.10 Aanduiding hoofdadres standplaats
                    57.11 Aanduiding nevenadressen standplaats
                    57.20 Standplaatsgeometrie
                    57.93 Aanduiding standplaatsgegevens in onderzoek
                    57.91 Datum begin geldigheid standplaatsgegevens
                    57.92 Datum einde geldigheid standplaatsgegevens
                 !! 57.97 Documentdatum mutatie standplaats
                 !! 57.98 Documentnummer mutatie standplaats
                 */
                table_name = "standplaats",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "officieel",
                    "standplaatsstatus",
                    "hoofdadres_id",
                    "nevenadres_ids",
                    "standplaatsgeometrie",
                    "inonderzoek",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                },
                element_name = BAG_LVC_Standplaats,
                parse = e => {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    var rel_ads = e.Element(BAG_LVC_gerelateerdeAdressen);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                                    // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,          // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),             // correctienummer
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                         // officieel
                        e.Element(BAG_LVC_standplaatsStatus).Value,                                // standplaatsstatus
                        rel_ads.Element(BAG_LVC_hoofdadres).Element(BAG_LVC_Identificatie).Value,  // hoofdadres_id
                        rel_ads.Elements(BAG_LVC_nevenadres).Select(
                            na => na.Element(BAG_LVC_Identificatie).Value
                        ).ToList(),                                                                // nevenadres_ids
                        parseGML(e.Element(BAG_LVC_standplaatsGeometrie).Elements().Single()),     // standplaatsgeometrie
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                       // inonderzoek
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),           // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),           // einddatumtijdvakgeldigheid
                   };
                },
            },
            ["LIG"] = new ImportParams(){
                /*
                    58.01 Ligplaatsidentificatie
                    58.02 Indicatie geconstateerde ligplaats
                    58.03 Ligplaatsstatus
                    58.10 Aanduiding hoofdadres ligplaats
                    58.11 Aanduiding nevenadressen ligplaats
                    58.20 Ligplaatsgeometrie
                    58.91 Datum begin geldigheid ligplaatsgegevens
                    58.93 Aanduiding ligplaatsgegevens in onderzoek
                    58.97 Documentdatum mutatie ligplaats
                    58.98 Documentnummer mutatie ligplaats
                */
                table_name = "ligplaats",
                column_names = new[] {
                    "identificatie",
                    "inactief",
                    "correctienummer",
                    "officieel",
                    "ligplaatsstatus",
                    "hoofdadres_id",
                    "nevenadres_ids",
                    "ligplaatsgeometrie",
                    "inonderzoek",
                    "begindatumtijdvakgeldigheid",
                    "einddatumtijdvakgeldigheid",
                },
                element_name = BAG_LVC_Ligplaats,
                parse = e => {
                    var tv = e.Element(BAG_LVC_Tijdvakgeldigheid);
                    var rel_ads = e.Element(BAG_LVC_gerelateerdeAdressen);
                    return new object[] {
                        e.Element(BAG_LVC_Identificatie).Value,                                    // identificatie
                        parseJN(e.Element(BAG_LVC_AanduidingRecordInactief).Value).Value,          // inactief
                        int.Parse(e.Element(BAG_LVC_AanduidingRecordCorrectie).Value),             // correctienummer
                        parseJN(e.Element(BAG_LVC_Officieel).Value).Value,                         // officieel
                        e.Element(BAG_LVC_ligplaatsStatus).Value,                                // liglaatsstatus
                        rel_ads.Element(BAG_LVC_hoofdadres).Element(BAG_LVC_Identificatie).Value,  // hoofdadres_id
                        rel_ads.Elements(BAG_LVC_nevenadres).Select(
                            na => na.Element(BAG_LVC_Identificatie).Value
                        ).ToList(),                                                                // nevenadres_ids
                        parseGML(e.Element(BAG_LVC_ligplaatsGeometrie).Elements().Single()),     // ligplaatsgeometrie
                        parseJN(e.Element(BAG_LVC_InOnderzoek).Value).Value,                       // inonderzoek
                        parseDate(tv.Element(BAGyypebegindatumTijdvakGeldigheid).Value),           // begindatumtijdvakgeldigheid
                        parseDate(tv.Element(BAGyypeeinddatumTijdvakGeldigheid)?.Value),           // einddatumtijdvakgeldigheid
                   };
                },
            },
            ["Perceel"] = new ImportParams(){
                table_name = "perceel",
                column_names = new[] {
                    "identificatie",
                    "geometrie",
                },
                element_name = brk_krt_Perceel,
                parse = e => {
                    /*
      <identificatie.NEN3610ID.lokaalID
      <historie>
        <KadasterHistorie>
          <logischTijdstipOntstaan>2009-03-27T23:59:59.000</logischTijdstipOntstaan>
        </KadasterHistorie>
      </historie>
      <kadastraleAanduiding>
        <TypeKadastraleAanduiding>
          <kadastraleGemeente>
            <KadastraleGemeenteKeuze>
              <AKRKadastraleGemeenteCode>HLM03</AKRKadastraleGemeenteCode>
            </KadastraleGemeenteKeuze>
          </kadastraleGemeente>
          <sectie>AO</sectie>
          <perceelnummer>733</perceelnummer>
        </TypeKadastraleAanduiding>
      </kadastraleAanduiding>
      <begrenzingPerceel>...GML...</begrenzingPerceel>
      <kadastraleGrootte>
        <TypeOppervlak>
          <waarde>184320</waarde>
        </TypeOppervlak>
      </kadastraleGrootte>
      <perceelnummerRotatie>0</perceelnummerRotatie>
      <perceelnummerVerschuiving>
        <TypePerceelnummerVerschuiving>
          <deltaX>0</deltaX>
          <deltaY>0</deltaY>
        </TypePerceelnummerVerschuiving>
      </perceelnummerVerschuiving>
      <plaatscoordinaten>
        <p1:Point srsName="urn:ogc:def:crs:EPSG::28992" p1:id="LOCAL_ID_1">
          <p1:pos>101265.542 470363.422</p1:pos>
        </p1:Point>
      </plaatscoordinaten>
    </Perceel>
                     */

                    return new object[] {
                        e.Element(brk_krt_identificatie)
                         .Element(brk_krt_NEN3610ID)
                         .Element(brk_krt_lokaalID).Value,                                      // identificatie
                        parseGML(e.Element(brk_krt_begrenzingPerceel).Elements().Single()),     // geometrie
                   };
                },
            },
        };

        public static void Import(IEnumerable<System.IO.Stream> input_streams, Action<string> log, string key, int total_streams, int? max_files_per_type)
        {
            var p = ParamsDict[key];

            log("Parsing XML");

            var todo = Math.Min(max_files_per_type ?? total_streams, total_streams);

            var c = new BlockingCollection<object[]>(1000);

            var t1 = Task.Factory.StartNew(
                () => TruncateAndCopy(table: p.table_name, columns: p.column_names, data: c, log: log),
                TaskCreationOptions.AttachedToParent
            );

            var t2 = Task.Factory.StartNew(
                () =>
                {
                    try
                    {
                        int n = 0;
                        int r = 0;
                        foreach (var input_stream in input_streams)
                        {
                            var reader = XmlReader.Create(input_stream);
                            reader.MoveToContent();
                            while (reader.Read())
                            {
                                while (reader.NodeType == XmlNodeType.Element
                                    && reader.NamespaceURI == p.element_name.Namespace
                                    && reader.LocalName == p.element_name.LocalName)
                                {
                                    XElement e = XNode.ReadFrom(reader) as XElement;
                                    c.Add(p.parse(e));
                                    r++;
                                    if (n == 0 && r % 100000 == 0)
                                        log($"{r,9} records {c.Count,9} items queued");
                                }
                            }
                            log($"{++n,5}/{todo,5} files written, {r,9} records {c.Count,9} items queued");
                            input_stream.Dispose();

                            if (max_files_per_type.HasValue && n >= max_files_per_type.Value)
                                break;
                        }
                        log($"Done writing to queue: {c.Count} items left");
                    }
                    finally
                    {
                        log("Cleanup: CompleteAdding()");
                        c.CompleteAdding();
                    }
                }
            );
            Task.WaitAll(new[] { t1, t2 });
            log("All done");
        }

        private static void TruncateAndCopy(string table, string[] columns, BlockingCollection<object[]> data, Action<string> log)
        {
            try
            {
                log("Connecting to database");
                using (var c = new Npgsql.NpgsqlConnection("Host=/var/run/postgresql;Port=5433"))
                {
                    c.Open();
                    using (var t = c.BeginTransaction())
                    {
                        log($"TRUNCATE {Q(table)}");
                        using (var cmd = new NpgsqlCommand($"TRUNCATE {Q(table)}", c))
                            cmd.ExecuteNonQuery();

                        log($"COPY {Q(table)}");

                        Copy(
                            c,
                            tablename: table,
                            colnames: columns,
                            data: data.GetConsumingEnumerable()
                        );
                        log("COMMIT");
                        t.Commit();
                    }
                }
            }
            finally
            {
                log("Disposing data...");
                data.CompleteAdding();
                data.Dispose();
            }
        }

        static XNamespace GML = "http://www.opengis.net/gml";
        static XNamespace GML32 = "http://www.opengis.net/gml/3.2";
        static XName GMLMultiSurface = GML + "MultiSurface";
        static XName GMLsurfaceMember = GML + "surfaceMember";
        static XName GMLPolygon = GML + "Polygon";
        static XName GML32Polygon = GML32 + "Polygon";
        static XName GMLexterior = GML + "exterior";
        static XName GML32exterior = GML32 + "exterior";
        static XName GMLinterior = GML + "interior";
        static XName GML32interior = GML32 + "interior";
        static XName GMLLinearRing = GML + "LinearRing";
        static XName GML32LinearRing = GML32 + "LinearRing";
        static XName GMLposList = GML + "posList";
        static XName GML32posList = GML32 + "posList";
        static XName GMLPoint = GML + "Point";
        static XName GMLpos = GML + "pos";

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
            else if (gmlElement.Name == GML32Polygon)
            {
                var exterior = gmlElement.Elements(GML32exterior).Single();
                var interiors = gmlElement.Elements(GML32interior);
                var iors = (new[] { exterior }).Union(interiors);
                return new PostgisPolygon(iors.Select(e => parseGMLRing(e.Elements(GML32LinearRing).Single()))) { SRID = srid };
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
            else if (gmlElement.Name == GMLPoint)
            {
                var pos = gmlElement.Elements(GMLpos).Single();
                var numbers = pos.Value.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (numbers.Length != 2)
                    if ((numbers.Length !=3) && double.Parse(numbers[2]) != 0.0)
                        throw new NotImplementedException("taarts");
                return new PostgisPoint(double.Parse(numbers[0]), double.Parse(numbers[1])) { SRID = srid };
            }
            else
                throw new NotImplementedException($"No pg conversion for {gmlElement.Name}");

        }

        private static Coordinate2D[] parseGMLRing(XElement ringElement)
        {
            var poslist = ringElement.Elements(GMLposList).SingleOrDefault()
                          ?? ringElement.Elements(GML32posList).Single();
            var ndims = int.Parse(poslist.Attribute("srsDimension").Value);
            var numbers = poslist.Value.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (ndims == 2)
            {
                var r = new Coordinate2D[numbers.Length / 2];
                for (int i = 0; i < r.Length; i++)
                {
                    r[i] = new Coordinate2D(double.Parse(numbers[i * 2]), double.Parse(numbers[i * 2 + 1]));
                }
                return r;
            }
            else if (ndims == 3)
            {
                var r = new Coordinate2D[numbers.Length / 3];
                for (int i = 0; i < r.Length; i++)
                {
                    var x = double.Parse(numbers[i * 3]);
                    var y = double.Parse(numbers[i * 3 + 1]);
                    var z = double.Parse(numbers[i * 3 + 2]);
                    if (z != 0)
                        throw new NotImplementedException("taartz");
                    r[i] = new Coordinate2D(x, y);
                }
                return r;
            }
            else
                throw new NotImplementedException("taart");
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

        static Func<string, string> Q = n => "\"" + n.Replace("\"", "\"\"") + "\"";

        static void Copy(NpgsqlConnection conn, string tablename, string[] colnames, IEnumerable<object[]> data)
        {
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
