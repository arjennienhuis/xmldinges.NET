using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Extensions.CommandLineUtils;

namespace BagDownloader
{
    class Program
    {

        const string ZIPFILE_FILENAME = "inspireadressen.zip";
        const string TIMESTAMP_FILENAME = "inspireadressen.zip.timestamp";

        static int Main(string[] args)
        {
            var app = new CommandLineApplication();
            app.Name = nameof(BagDownloader);

            app.HelpOption("--help");
            var pathArgument = app.Option("--path", "Download path", CommandOptionType.SingleValue);
            var forceArgument = app.Option("--force", "Force the download", CommandOptionType.NoValue);

            app.OnExecute(() => run());
            return app.Execute(args);

            void Log(string msg) => Console.WriteLine(msg);

            int Error(string msg)
            {
                Console.Error.WriteLine(msg);
                return -1;
            }

            async Task<int> run()
            {
                var base_path = pathArgument.Value();
                if (base_path == null)
                    return Error("Missing argument: --path");

                string existing_updated;
                if (forceArgument.HasValue())
                {
                    existing_updated = null;
                }
                else
                {
                    existing_updated = ReadUpdated(base_path: base_path, ignore_not_found: true);
                    if (existing_updated == null)
                        return Error("Exsisting download not found. Use --force to download a new version.");
                }

                var (has_downloaded, new_updated) = await Downloader.ConditionalDownload(
                    existing_updated: existing_updated,
                    output_path: Path.Combine(base_path, ZIPFILE_FILENAME),
                    Log: Log
                );

                if (has_downloaded)
                {
                    WriteUpdated(base_path: base_path, updated: new_updated);
                }

                return 0;
            }
        }

        private static string ReadUpdated(string base_path, bool ignore_not_found = false)
        {
            var path = Path.Combine(base_path, TIMESTAMP_FILENAME);
            try
            {
                return File.ReadAllText(path: path);
            }
            catch (FileNotFoundException) when (ignore_not_found)
            {
                return null;
            }
        }

        private static void WriteUpdated(string base_path, string updated)
        {
            var path = Path.Combine(base_path, TIMESTAMP_FILENAME);
            File.WriteAllText(path: path, contents: updated);
        }
    }
}
