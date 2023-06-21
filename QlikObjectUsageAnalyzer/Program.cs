using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            var opMonId = "<id>";
            var apiKey = "<key>";

            var location = QcsLocation.FromUri(url);
            location.AsApiKey(apiKey);

            var client = new RestClient(url);
            client.AsApiKeyViaQcs(apiKey);
            var allAppIds = GetAllAppIds(client).ToArray();
            // return;
            var sheetUsage = GetSheetUsage(location, opMonId).Result;

            var sw = new Stopwatch();
            sw.Start();
            var allContents = ScanAllApps(location, allAppIds.Where(id => id != opMonId).ToArray()).Result;
            PrintSheetContentsTable(allContents, sheetUsage);
            sw.Stop();
            WriteLine("Total time: " + sw.Elapsed);
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanAllApps(IQcsLocation location, string[] allAppIds)
        {
            var result = new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            foreach (var appId in allAppIds)
            {
                var contents = await ScanApp(location, appId);
                foreach (var (key,content) in contents)
                {
                    result[key] = content;
                }
            }
            return result;
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanApp(IQcsLocation location, string appId)
        {
            Write($"Connecting to app: {appId}... ");
            using (var app = await location.AppAsync(appId, SessionToken.Unique(), noData: true))
            {
                WriteLine("Done!");
                var sheetContents = await GetSheetContents(appId, app);
                WriteLine($"Sheet contents collected for app: {appId}");
//                PrintSheetContents(sheetContents);
                // PrintSheetContentsTable(appId, sheetContents, sheetUsage);
                return sheetContents;
            }
        }

        private static IEnumerable<string> GetAllAppIds(IRestClient client)
        {
            var next = client.Url + "/api/v1/items?resourceType=app";
            while (!string.IsNullOrWhiteSpace(next))
            {
                // WriteLine(next);
                // WriteLine(next.Substring(client.Url.Length));
                var rsp = client.Get(next.Substring(client.Url.Length));
                // WriteLine(rsp);
                var appInfos = JToken.Parse(rsp);
                foreach (var appInfo in appInfos["data"].OfType<JObject>())
                {
                    var title = appInfo["name"].Value<string>();
                    var id = appInfo["resourceId"].Value<string>();
                    WriteLine($"  {id}: {title}");
                    // WriteLine(appInfo.ToString());
                    yield return id;
                }
                // WriteLine(appInfos["links"].ToString());
                next = appInfos["links"]["next"]?["href"].Value<string>();
            }
        }

        private static async Task<Dictionary<(string, string), int>> GetSheetUsage(IQcsLocation location, string opMonId)
        {
            var result = new Dictionary<(string, string), int>();
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
            WriteLine(string.Join("\t", headers));
            foreach (var item in sheetContents)
            {
                int usage;
                if (!sheetUsage.TryGetValue(item.Key, out usage))
                    usage = 0;
                        
                foreach (var row in item.Value.Select(o => $"{item.Key.Item1}\t{item.Key.Item2}\t{usage}\t{o.Pretty()}"))
                {
                    WriteLine(row);
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

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> GetSheetContents(string appId, IApp app)
        {
            var result = new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            using (var sheetList = await app.GetSheetListAsync())
            {
                var layout = await sheetList.GetLayoutAsync();
                var sheetItems = layout.AppObjectList.Items.ToArray();
                WriteLine($"Getting sheet contents for {sheetItems.Length} sheets.");
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

        private static void WriteLine(string msg = "")
        {
            Write(msg + Environment.NewLine);
        }

        private static void Write(string msg)
        {
            Console.Write(msg);
        }
    }
}
