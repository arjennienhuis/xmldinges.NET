
using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace BagDownloader
{
    public static class Downloader
    {
        public async static Task<(bool has_downloaded, string new_updated)> ConditionalDownload(string existing_updated, string output_path, Action<string> Log = null)
        {
            var atom = await GetAtom();
            if (existing_updated != null)
            {
                Log?.Invoke($"Old version: {existing_updated}, New version: {atom.updated}");

                if (existing_updated == atom.updated)
                {
                    Log?.Invoke("Already up to date. Skiping download.");
                    return (has_downloaded: false, new_updated: atom.updated);
                }
            }
            else
            {
                Log?.Invoke($"Force downloading new version: {atom.updated}");
            }

            Log?.Invoke("Downloading new version...");
            Log?.Invoke(atom.url);
            await DownloadFile(url: atom.url, filePath: output_path);
            Log?.Invoke("Done.");
            return (has_downloaded: true, new_updated: atom.updated);
        }

        public async static void Download(string output_path, Action<string> Log = null)
        {
            await ConditionalDownload(output_path: output_path, Log: Log, existing_updated: null);
        }

        static async Task<(string updated, string url)> GetAtom()
        {
            XNamespace NS_ATOM = "http://www.w3.org/2005/Atom";
            const string ATOM_URL = "http://geodata.nationaalgeoregister.nl/inspireadressen/atom/inspireadressen.xml";
            string atom = await UsingAsync(() => new HttpClient(), hc => hc.GetStringAsync(ATOM_URL));
            var doc = XDocument.Parse(atom);
            var atom_entry = doc.Descendants(NS_ATOM + "entry").Single();
            string updated = atom_entry.Element(NS_ATOM + "updated").Value;
            string url = atom_entry.Element(NS_ATOM + "link").Attribute("href").Value;
            return (updated: updated, url: url);
        }

        static async Task DownloadFile(string url, string filePath)
        {
            var tmpPath = filePath + ".part";
            using (var f = new FileStream(tmpPath, FileMode.Create,FileAccess.Write, FileShare.None))
            using (var hc = new HttpClient())
            {
                var response = await hc.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                var s = await response.Content.ReadAsStreamAsync();
                await s.CopyToAsync(f, 1024 * 1024 * 4);
            }
            File.Replace(tmpPath, filePath, destinationBackupFileName: null);
        }

        static async Task<T> UsingAsync<D, T>(Func<D> df, Func<D, Task<T>> task) where D : IDisposable
        {
            using (var d = df())
                return await task(d);
        }
    }
}

