# qlik-object-usage-analyzer

Tool for scanning a Qlik SaaS tenant for which object types are used in each app.

```
Usage: QlikObjectUsageAnalyzer.exe <url> (<apiKey> | <cert folder>) [<help>] [<verbose>] [<workers>] [<output file>]
  <help>        : (-h | --help) <string>, Print this message.
  <url>         : (-u | --url) <string>, Url to connect to.
  <apiKey>      : (-a | --apiKey) <string>, For connecting to QCS
  <cert folder> : (-c | --certFolder) <file path>, For connecting to Client Managed
  <workers>     : (-w | --workers) int, Number of apps to analyze concurrently. Default: 8
  <output file> : (-o | --outputFile) <file path>, Default: Write to stdout only.
  <verbose>     : (-v | --verbose), Write result to stdout even when writing to file. Default: false
```
