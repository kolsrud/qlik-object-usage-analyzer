using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Qlik.Engine;
using Qlik.Sense.Client;
using Qlik.Sense.RestClient;

namespace QlikObjectUsageAnalyzer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var url = "<url>";
            var apiKey = "<key>";

            var location = QcsLocation.FromUri(url);
            location.AsApiKey(apiKey);

            var client = new RestClient(url);
            client.AsApiKeyViaQcs(apiKey);
            // return;

            Main(location, client).Wait();
        }

        private static async Task Main(IQcsLocation location, RestClient client)
        {
            var opMonId = "";
            var sheetUsage = await GetSheetUsage(location, opMonId);

            var allAppIds = (await GetAllAppIds(client)).ToArray();

            WriteLine($"Scanning {allAppIds.Length} apps.");
            // return;
            var sw = new Stopwatch();
            sw.Start();
            var allContents = await ScanAllApps(location, allAppIds);
            PrintSheetContentsTable(allContents, sheetUsage);
            sw.Stop();
            WriteLine("Total time: " + sw.Elapsed);
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanAllApps(IQcsLocation location, string[] allAppIds)
        {
            var result = new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            var workerPool = new WorkerPool<Dictionary<(string, string), IEnumerable<ObjectInfo>>>(8);
            foreach (var appId in allAppIds)
            {
                workerPool.AddWork(() => ScanApp(location, appId));
            }

            var cnt = 0;
            while (workerPool.HasWork)
            {
                var contents = await workerPool.GetResult();
                Write(GetSymbol(cnt++));
                foreach (var (key, content) in contents)
                {
                    result[key] = content;
                }
            }

            return result;
        }

        private static string GetSymbol(int i)
        {
            if (i % 100 == 0)
                return (i != 0 ? Environment.NewLine : "") + i + " |";
            if (i % 10 == 0)
                return "|";
            return ".";
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanApp(IQcsLocation location, string appId, bool verbose = false)
        {
            if (verbose) Write($"Connecting to app: {appId}... ");
            try
            {
                using (var app = await location.AppAsync(appId, SessionToken.Unique(), noData: true))
                {
                    if (verbose) WriteLine("Done!");
                    var sheetContents = await GetSheetContents(appId, app);
                    if (verbose) WriteLine($"Sheet contents collected for app: {appId}");
//                PrintSheetContents(sheetContents);
                    // PrintSheetContentsTable(appId, sheetContents, sheetUsage);
                    return sheetContents;
                }
            }
            catch
            {
                Write("e");
                return new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            }
        }

        private static async Task<IEnumerable<string>> GetAllAppIds(IRestClient client)
        {
            var next = client.Url + "/api/v1/items?resourceType=app&limit=100";
            var result = new List<string>();
            while (!string.IsNullOrWhiteSpace(next))
            {
                // WriteLine(next);
                // WriteLine(next.Substring(client.Url.Length));
                var rsp = await client.GetAsync(next.Substring(client.Url.Length));
                // WriteLine(rsp);
                var appInfos = JToken.Parse(rsp);
                foreach (var appInfo in appInfos["data"].OfType<JObject>())
                {
                    var title = appInfo["name"].Value<string>();
                    var id = appInfo["resourceId"].Value<string>();
                    WriteLine($"  {id}: {title}");
                    // WriteLine(appInfo.ToString());
                    result.Add(id);
                }
                // WriteLine(appInfos["links"].ToString());
                next = appInfos["links"]["next"]?["href"].Value<string>();
                // yield break;
                break;
            }

            return result;
        }

        private static async Task<Dictionary<(string, string), int>> GetSheetUsage(IQcsLocation location, string opMonId)
        {
            var result = new Dictionary<(string, string), int>();
            return result;
            using (var app = await location.AppAsync(opMonId, SessionToken.Unique()))
            {
                var cubeDef = CreateCubeDef();
                var props = new GenericObjectProperties { Info = new NxInfo { Type = "mytype" } };
                props.Set("qHyperCubeDef", cubeDef);
                var field = app.GetField("sheet_activity_last_90");
                // field.Select("1");
                // using (new DebugConsole())
                using (var o = await app.CreateGenericSessionObjectAsync(props))
                {
                    var pager = o.GetHyperCubePager("/qHyperCubeDef");
                    foreach (var row in pager.GetAllData())
                    {
                        var usage = (int) row[2].Num == 1 ? 3 : (int) row[3].Num == 1 ? 2 : (int) row[4].Num == 1 ? 1 : 0;
                        var key = (row[0].Text, row[1].Text);
                        result[key] = usage;
                        WriteLine($"Sheet usage: {key}: {usage}");
                    }
                }
            }
            return result;
        }

        private static HyperCubeDef CreateCubeDef()
        {
            var fields = new[]
                { "AppId", "AppObjectId", "sheet_activity_last_30", "sheet_activity_last_60", "sheet_activity_last_90" };
            
            return new HyperCubeDef
            {
                Dimensions = fields.Select(f => new NxDimension{Def = new NxInlineDimensionDef{FieldDefs = new[]{f}}}).ToArray()
            };
        }

        private static void PrintSheetContentsTable(Dictionary<(string, string), IEnumerable<ObjectInfo>> sheetContents, Dictionary<(string, string), int> sheetUsage)
        {
            var headers = new[] { "appId", "sheetId", "usage", "objectId", "objectType", "objectTitle" };
            WriteLine(string.Join("\t", headers), true);
            foreach (var item in sheetContents)
            {
                int usage;
                if (!sheetUsage.TryGetValue(item.Key, out usage))
                    usage = 0;
                        
                foreach (var row in item.Value.Select(o => $"{item.Key.Item1}\t{item.Key.Item2}\t{usage}\t{o.Pretty()}"))
                {
                    WriteLine(row, true);
                }
            }
        }

        private static void PrintSheetContents(Dictionary<(string, string), IEnumerable<ObjectInfo>> sheetContents)
        {
            WriteLine("Sheet contents:");
            foreach (var sheetContent in sheetContents)
            {
                var oInfos = sheetContent.Value.ToArray();
                WriteLine($"  AppID: {sheetContent.Key.Item1} Sheet ID: {sheetContent.Key.Item2} ({oInfos.Length} objects)");
                foreach (var oInfo in oInfos)
                {
                    WriteLine($"    {oInfo.Pretty()}");
                }
            }
        }

        class ObjectInfo
        {
            public string Id;
            public string Title;
            public string Type;

            public string Pretty()
            {
                return $"{Id}\t{Type}\t\"{Title}\"";
            }
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> GetSheetContents(string appId, IApp app, bool verbose = false)
        {
            var result = new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            using (var sheetList = await app.GetSheetListAsync())
            {
                var layout = await sheetList.GetLayoutAsync();
                var sheetItems = layout.AppObjectList.Items.ToArray();
                if (verbose) WriteLine($"Getting sheet contents for {sheetItems.Length} sheets.");
                foreach (var item in sheetItems)
                {
                    var sheetId = item.Info.Id;
                    // Write($"  Getting sheet contents for sheet: {sheetId}... ");
                    var objectInfos = (await GetSheetContents(app, sheetId)).ToArray();
                    // WriteLine($"Found {objectInfos.Length} children.");
                    result[(appId, sheetId)] = objectInfos;
                }
            }
            return result;
        }

        private static async Task<IEnumerable<ObjectInfo>> GetSheetContents(IApp app, string sheetId)
        {
            var sheet = await app.GetGenericObjectAsync(sheetId);
            var propertyTree = await sheet.GetFullPropertyTreeAsync();
            var allChildInfo = propertyTree.Children.SelectMany(GetAllProps);
            return allChildInfo.Select(GetObjectInfo);
        }

        private static IEnumerable<GenericObjectProperties> GetAllProps(GenericObjectEntry entry)
        {
            return new []{entry.Property}.Concat(entry.Children.SelectMany(GetAllProps));
        }

        private static ObjectInfo GetObjectInfo(GenericObjectProperties props)
        {
            return new ObjectInfo
            {
                Id = props.Info.Id,
                Title = props.MetaDef.Get<string>("title") ?? "<no title>",
                Type = props.Info.Type,
            };
        }

        private static void WriteLine(string msg = "", bool writeToFile = false)
        {
            Write(msg + Environment.NewLine, writeToFile);
        }

        private static StreamWriter _outputFile = new StreamWriter(@"C:\Tmp\objectScan.csv", false);

        private static void Write(string msg, bool writeToFile = false)
        {
            Console.Write(msg);
            if (writeToFile) _outputFile.Write(msg);
        }
    }
}
