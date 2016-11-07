using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.IO.Compression;
using ConsoleApplication;

namespace xmldinges.NET
{
    public class ProcessDir
    {
        public static void OpenAll(string basepath, Action<string> log, List<string> import_types, int? max_files_per_type)
        {
            Regex is_wpl = new Regex(@"^9999WPL[0-9]{8}\.zip$");
            Regex is_num = new Regex(@"^9999NUM[0-9]{8}\.zip$");
            Regex is_vbo = new Regex(@"^9999VBO[0-9]{8}\.zip$");
            Regex is_pnd = new Regex(@"^9999PND[0-9]{8}\.zip$");
            Regex is_gem_wpl = new Regex(@"^GEM-WPL-RELATIE-[0-9]{8}\.zip$");
            Regex is_zip = new Regex(@"^9999...[0-9]{8}\.zip$");

            var tasks = new List<Task>();

            foreach (var path in System.IO.Directory.EnumerateFileSystemEntries(basepath))
            {
                Action<Action<IEnumerable<ZipArchiveEntry>>> addziptask = t => tasks.Add(
                    Task.Factory.StartNew(
                        () => ProcessZip(path, t),
                        TaskCreationOptions.AttachedToParent
                    )
                );

                Action<string> addimporttask = key => addziptask(es => {
                    if (import_types.Count > 0 && !import_types.Contains(key))
                    {
                        log($"Skipping {key}");
                    }
                    else
                    {
                        Program.Import(
                            es.Select(e => e.Open()),
                            log: s => log($"{key}: {s}"),
                            key: key,
                            max_files_per_type: max_files_per_type
                        );
                    }
                });
                
                //Action<Action<IEnumerable<ZipArchiveEntry>>> addziptask = t => ProcessZip(path, t);
                var filename = System.IO.Path.GetFileName(path);
                if (filename == "Leveringsdocument-BAG-Extract.xml")
                {
                    // ignore...
                }
                else if (is_gem_wpl.IsMatch(filename))
                {
                    // ignore...
                }
                else if (is_num.IsMatch(filename))
                {
                    addimporttask("NUM");
                }
                else if (is_wpl.IsMatch(filename))
                {
                    addimporttask("WPL");
                }
                else if (is_vbo.IsMatch(filename))
                {
                    addimporttask("VBO");
                }
                else if (is_pnd.IsMatch(filename))
                {
                    addimporttask("PND");
                }
                else if (is_zip.IsMatch(filename))
                {
                    log($"skipping unkknown file: {filename}");
                }
                else throw new Exception($"Unknown file: {filename}");
            }

            foreach (var t in tasks)
                t.Wait();
            log("All Done.");
            Console.WriteLine("Press key to continue");
            Console.ReadKey();
        }

        static void ProcessZip(string filename, Action<IEnumerable<ZipArchiveEntry>> action)
        {
            using (var z = ZipFile.OpenRead(filename))
                action(z.Entries);
        }
    }
}
