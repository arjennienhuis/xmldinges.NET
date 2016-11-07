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
        public static void OpenAll(string basepath, Action<string> Log)
        {
            Regex is_wpl = new Regex(@"^9999WPL[0-9]{8}\.zip$");
            Regex is_num = new Regex(@"^9999NUM[0-9]{8}\.zip$");
            Regex is_vbo = new Regex(@"^9999VBO[0-9]{8}\.zip$");
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
                    addziptask(es => Program.Import(es.Select(e => e.Open()), Log: s => Log($"NUM: {s}"), key: "NUM"));
                }
                else if (is_wpl.IsMatch(filename))
                {
                    addziptask(es => Program.Import(es.Select(e => e.Open()), Log: s => Log($"WPL: {s}"), key: "WPL"));
                }
                else if (is_vbo.IsMatch(filename))
                {
                    addziptask(es => Program.Import(es.Select(e => e.Open()), Log: s => Log($"VBO: {s}"), key: "VBO"));
                }
                else if (is_zip.IsMatch(filename))
                {
                }
                else throw new Exception($"Unknown file: {filename}");
                Console.WriteLine(filename);
            }

            foreach (var t in tasks)
                t.Wait();
            Log("All Done.");
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
