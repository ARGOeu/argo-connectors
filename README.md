**argo-egi-connectors** is a bundle of connectors/sync components for various data sources established in EGI infrastructure, most notably GOCDB (topology, downtimes), but there's also support for alternative topology grabbing via various VO feeds and weights information via GStat service.

Data is synced in a daily manner and all together with a prefiltered and expanded status messages from _argo-egi-consumer_ represents an input for _argo-compute-engine_.

Configuration of connectors is centered around two configuration files:
- `global.conf` - URLs of sources besides GOCDB and VO, output filenames, avro schemas
- `customer.conf` - enlisted jobs for EGI, enlisted all VO'es and their jobs, job attributes and an implicit directory structure where connectors put their data

More info: http://argoeu.github.io/guides/sync/
