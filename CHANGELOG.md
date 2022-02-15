# Changelog

## [2.0.0] - 2022-02-10

* ARGO-3375 Pass contacts for each topology entity
* ARGO-3427 Fetch HOSTDN service attribute from GOCDB
* ARGO-3428 Fetch SE_PATH from BDII
* ARGO-3448 NEANIAS dulpicates in group topology
* ARGO-3503 Support multiple VO SE_PATHs for host
* ARGO-3521 Retry on empty responses
* ARGO-3522 Introduce optional scope key for topology fetch and BIOMED feed parse
* ARGO-3524 Improve exception handling
* ARGO-3528 Verbose retry log messages
* ARGO-3540 Tests for retry logic

### Added

* ARGO-2620 Update connectors configuration templates
* ARGO-2622 Populate group_type field for WEB-API weights
* ARGO-3064 Introduce async topology CSV
* ARGO-3215 Pass EXTENSIONS bucket in new connectors
* ARGO-3295 Add GOCDB URLs
* ARGO-3301 Query BDII and extract SRM port
* ARGO-3335 Retry on LDAP queries
* ARGO-3340 Unit testing of parsing of GOCDB service endpoint
* ARGO-3341 Pass date argument to WEB-API methods

### Fixed

* ARGO-2621 Detect missing path separator for tenant directory avros
* ARGO-2660 Basic HTTP authentication not correctly set up
* ARGO-2681 Weights use HTTP PUT to write new daily entries
* ARGO-2771 Weight connector issues investigated
* ARGO-3268 Downtimes DELETE with explicit date passed

### Changed

* ARGO-2591 Connectors CentOS7 and Py3 migrate
* ARGO-2596 Push sync_data to WEB-API
* ARGO-2619 Remove Centos 6 RPM building
* ARGO-2656 Downtimes as global tenant resource
* ARGO-2860 Separate connection, parsing and file handling
* ARGO-2861 Switch to async IO operations
