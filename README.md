# qlik-object-usage-analyzer

Tool for scanning a Qlik SaaS tenant for which object types are used in each app.

```
Usage: QlikObjectUsageAnalyzer <url> <apiKey> [<verbose>] [<workers>] [<output file>]
  <url>         : (-u | --url) <string>
  <apiKey>      : (-a | --apiKey) <string>
  <workers>     : (-w | --workers) int, Number of apps to analyze concurrently. Default: 8
  <output file> : (-o | --outputFile) <file path>, Default: Write to stdout only.
  <verbose>     : (-v | --verbose), Write result to stdout even when writing to file. Default: false
```
