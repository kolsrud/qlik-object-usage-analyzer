﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Qlik.Engine;
using Qlik.Engine.Communication;
using Qlik.Sense.Client;
using Qlik.Sense.RestClient;

namespace QlikObjectUsageAnalyzer
{
    internal class Program
    {
	    private class Arguments
	    {
		    public string Url;
		    public string ApiKey;
            public string CertFolder;
		    public int Workers = 8;
		    public string OutputFile = null;
		    public bool Verbose = false;

		    public static Arguments ProcessArgs(string[] args)
		    {
			    var error = false;
                var result = new Arguments();
                for (int i = 0; i < args.Length; i++)
                {
	                switch (args[i])
	                {
                        case "-h":
                        case "--help":
                            PrintUsage();
                            Environment.Exit(1);
                            break;
		                case "-u":
		                case "--url": result.Url = args[i + 1]; i++; break;
		                case "-a":
		                case "--apiKey": result.ApiKey = args[i + 1]; i++; break;
                        case "-c":
                        case "--certFolder": result.CertFolder = args[i + 1]; i++; break;
		                case "-w":
		                case "--workers": result.Workers = int.Parse(args[i + 1]); i++; break;
		                case "-o":
		                case "--outputFile": result.OutputFile = args[i + 1]; i++; break;
		                case "-v":
		                case "--verbose": result.Verbose = true; break;
                        default:
	                        Console.WriteLine($"Error processing arguments. Unknown argument {args[i]}");
                            error = true;
	                        break;
					}
                }

                if (result.Url == null)
                {
	                Console.WriteLine($"Error processing arguments. Url not defined.");
	                error = true;
                }
                if (result.ApiKey == null && result.CertFolder == null)
                {
	                Console.WriteLine($"Error processing arguments. One of ApiKey or CertFolder must be defined.");
	                error = true;
                }

                if (error)
                {
	                PrintUsage();
	                Environment.Exit(1);
                }

                return result;
		    }

		    private static void PrintUsage()
		    {
			    var exe = System.AppDomain.CurrentDomain.FriendlyName;
				Console.WriteLine($"Usage: {exe} <url> (<apiKey> | <cert folder>) [<help>] [<verbose>] [<workers>] [<output file>]");
                Console.WriteLine($"  <help>        : (-h | --help) <string>, Print this message.");
				Console.WriteLine($"  <url>         : (-u | --url) <string>, Url to connect to.");
				Console.WriteLine($"  <apiKey>      : (-a | --apiKey) <string>, For connecting to QCS");
                Console.WriteLine($"  <cert folder> : (-c | --certFolder) <file path>, For connecting to Client Managed");
				Console.WriteLine($"  <workers>     : (-w | --workers) int, Number of apps to analyze concurrently. Default: 8");
				Console.WriteLine($"  <output file> : (-o | --outputFile) <file path>, Default: Write to stdout only.");
				Console.WriteLine($"  <verbose>     : (-v | --verbose), Write result to stdout even when writing to file. Default: false");
		    }
		}

        private static bool _verbose = true;

        static void Main(string[] args)
        {
	        var config = Arguments.ProcessArgs(args);
            Func<string, bool, Task<IApp>> getAppFunc;
            Func<Task<IEnumerable<string>>> getAllAppIdsFunc;

            if (config.ApiKey != null)
            {
                (getAppFunc, getAllAppIdsFunc) = ConnectToQcs(config);
            }
            else
            {
                (getAppFunc, getAllAppIdsFunc) = ConnectToClientManaged(config);
            }

            WriteLine($"Scanning object usage for URL: {config.Url}");
            WriteLine($"Using {config.Workers} workers.");
			
            if (config.OutputFile != null)
            {
	            WriteLine($"Writing result to file: {config.OutputFile}");
                _outputFile = new StreamWriter(config.OutputFile, false);
                _verbose = config.Verbose;
			}
            Main(getAppFunc, getAllAppIdsFunc).Wait();
        }

        private static (Func<string, bool, Task<IApp>> getAppFunc, Func<Task<IEnumerable<string>>> getAllAppIdsFunc) ConnectToClientManaged(Arguments config)
        {
            WriteLine("Connecting to client managed using certificates.");

            var certs = CertificateManager.LoadCertificateFromDirectory(config.CertFolder);

            var location = Location.FromUri(config.Url);
            location.AsDirectConnection("INTERNAL", "sa_api", certs, false);
            location.CustomUserAgent = System.AppDomain.CurrentDomain.FriendlyName;

            var client = new RestClient(config.Url);
            client.AsDirectConnection("INTERNAL", "sa_api", 4242, false, certs);
            client.CustomUserAgent = System.AppDomain.CurrentDomain.FriendlyName;

            Func<string, bool, Task<IApp>> getAppFunc = (appId, noData) => location.AppAsync(appId, Session.Random, noData);
            Func<Task<IEnumerable<string>>> getAllAppIdsFunc = () => GetAllAppIdsClientManaged(client);

            return (getAppFunc, getAllAppIdsFunc);
        }

        private static (Func<string, bool, Task<IApp>> getAppFunc, Func<Task<IEnumerable<string>>> getAllAppIdsFunc) ConnectToQcs(Arguments config)
        {
            WriteLine("Connecting to QCS using api key.");
            
            var location = QcsLocation.FromUri(config.Url);
            location.AsApiKey(config.ApiKey);
            location.CustomUserAgent = System.AppDomain.CurrentDomain.FriendlyName;

            var client = new RestClient(config.Url);
            client.AsApiKeyViaQcs(config.ApiKey);
            client.CustomUserAgent = System.AppDomain.CurrentDomain.FriendlyName;

            Func<string, bool, Task<IApp>> getAppFunc = (appId, noData) => location.AppAsync(appId, SessionToken.Unique(), noData);
            Func<Task<IEnumerable<string>>> getAllAppIdsFunc = () => GetAllAppIdsQcs(client);

            return (getAppFunc, getAllAppIdsFunc);
        }

        private static async Task Main(Func<string, bool, Task<IApp>> getAppFunc, Func<Task<IEnumerable<string>>> getAllAppIdsFunc)
        {
            // var opMonId = "";
            // var sheetUsage = await GetSheetUsage(location, opMonId);
            var sheetUsage = await GetSheetUsage();

            var allAppIds = (await getAllAppIdsFunc()).ToArray();

            WriteLine($"Scanning {allAppIds.Length} apps.");
            // return;
            var sw = new Stopwatch();
            sw.Start();
            var allContents = await ScanAllApps(getAppFunc, allAppIds, 8);
            PrintSheetContentsTable(allContents, sheetUsage);
            sw.Stop();
            WriteLine($"Found {allContents.Count} objects.");
            WriteLine("Total time: " + sw.Elapsed);
        }

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanAllApps(Func<string, bool, Task<IApp>> getAppFunc, string[] allAppIds, int workers)
        {
            var result = new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            var workerPool = new WorkerPool<Dictionary<(string, string), IEnumerable<ObjectInfo>>>(workers);
            foreach (var appId in allAppIds)
            {
                workerPool.AddWork(() => ScanApp(getAppFunc, appId, false));
            }

            var cnt = 0;
            while (workerPool.HasWork)
            {
                var contents = await workerPool.GetResult();
                Write(GetSymbol(cnt++));
                foreach (var kv in contents)
                {
                    result[kv.Key] = kv.Value;
                }
            }

            WriteLine();
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

        private static async Task<Dictionary<(string, string), IEnumerable<ObjectInfo>>> ScanApp(Func<string, bool, Task<IApp>> getAppFunc, string appId, bool verbose = false)
        {
            if (verbose) Write($"Connecting to app: {appId}... ");
            try
            {
                using (var app = await getAppFunc(appId, true))
                {
                    if (verbose) WriteLine("Done!");
                    var sheetContents = await GetSheetContents(appId, app);
                    if (verbose) WriteLine($"Sheet contents collected for app: {appId}");
                    // PrintSheetContents(sheetContents);
                    // PrintSheetContentsTable(appId, sheetContents, sheetUsage);
                    return sheetContents;
                }
            }
            catch (Exception ex)
            {
                WriteLine($"Error during analysis of app {appId}: {ex}");
                return new Dictionary<(string, string), IEnumerable<ObjectInfo>>();
            }
        }

        private static async Task<IEnumerable<string>> GetAllAppIdsClientManaged(IRestClient client)
        {
            var allAppInfo = await client.GetAsync<JArray>("/qrs/app");
            return allAppInfo.OfType<JObject>().Select(info => info["id"].Value<string>()).ToArray();
        }

        private static async Task<IEnumerable<string>> GetAllAppIdsQcs(IRestClient client)
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
                    // WriteLine($"  {id}: {title}");
                    // WriteLine(appInfo.ToString());
                    result.Add(id);
                }

                try
                {
	                var row = Console.CursorTop;
	                Console.SetCursorPosition(0, row);
                }
                catch
                {
                    WriteLine();
                }

                Write("Apps found: " + result.Count);
                // WriteLine(appInfos["links"].ToString());
                next = appInfos["links"]["next"]?["href"].Value<string>();
            }
            Console.WriteLine();
            return result;
        }

        private static async Task<Dictionary<(string, string), int>> GetSheetUsage()
        {
            var result = new Dictionary<(string, string), int>();
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

        private static StreamWriter _outputFile = null;

        private static void Write(string msg, bool writeToFile = false)
        {
            if (_verbose || !writeToFile) Console.Write(msg);
            if (writeToFile && _outputFile != null) _outputFile.Write(msg);
        }
    }
}
