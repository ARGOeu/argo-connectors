# Changelog

## [2.2.2] - 2023-02-01

### Fixed

* ARGO-4158 Topology connector doesn't honor notification flag for EGI

### Changed

* ARGO-4127 Switch to private API methods of PROVIDERS portal
* ARGO-4136 Replace avro writer with plain JSONs

### Added

* ARGO-4101 Connectors should sort service types before POST
* ARGO-4102 Connectors POST service types with tags=connectors field set
* ARGO-4142 Connectors should update only service types with tags=topology

## [2.2.1] - 2022-11-03

### Added

* ARGO-4011 Fetch Horizontal services from PROVIDERS portal

### Fixed

* ARGO-4057 Downtimes GOCDB connector does not take into account AuthN options
* ARGO-4099 Do not try to parse EXTENSIONS for GOCDB sub services

## [2.2.0] - 2022-07-28

### Added

* ARGO-3695 [NEANIAS] Use ARGO for downtimes

## [2.1.0] - 2022-06-07

### Added

* ARGO-2862 Introduce topo, weights, downtimes and metricprofile individual async tasks
* ARGO-3583 Add support in Connectors for EOSC Providers Portal API
* ARGO-3666 Introduce service type and descriptions fetch
* ARGO-3700 EOSC topology service endpoints with only one hardcoded service for start
* ARGO-3803 Use id as a service group name for EOSC PROVIDER topology
* ARGO-3818 Use id as a project name for EOSC PROVIDER topology instead of abbreviation
* ARGO-3849 Fetch monitoring extensions

### Fixed

* ARGO-3708 EOSCPROVIDER contacs is wrongly populated with one contact for every different endpoint
* ARGO-3858 Slow performance with mesh of contacts data for EOSCPROVIDER topo
* ARGO-3861 Some of endpoints are skipped if multiple endpoints are defined for the same resource on service-extensions
* ARGO-3866 EOSC topology issue when defining two endpoints with the same hostname/service-type through monitoring extensions
* ARGO-3869 Strip port from extracted hostname in group endpoints

### Changed

* ARGO-3842 Rename py modules and pkg to argo-connectors
* ARGO-3846 Do not assume first fetch with paginated indexes succesfull
* ARGO-3855 Join error lines for PROVIDER topology
* ARGO-3856 find_next_paging_cursor_count for GOCDB topo task implies succesfull parse

## [2.0.0] - 2022-02-10

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
* ARGO-3375 Pass contacts for each topology entity
* ARGO-3427 Fetch HOSTDN service attribute from GOCDB
* ARGO-3428 Fetch SE_PATH from BDII
* ARGO-3522 Introduce optional scope key for topology fetch and BIOMED feed parse
* ARGO-3540 Tests for retry logic

### Fixed

* ARGO-2621 Detect missing path separator for tenant directory avros
* ARGO-2660 Basic HTTP authentication not correctly set up
* ARGO-2681 Weights use HTTP PUT to write new daily entries
* ARGO-2771 Weight connector issues investigated
* ARGO-3268 Downtimes DELETE with explicit date passed
* ARGO-3448 NEANIAS dulpicates in group topology
* ARGO-3503 Support multiple VO SE_PATHs for host

### Changed

* ARGO-2591 Connectors CentOS7 and Py3 migrate
* ARGO-2596 Push sync_data to WEB-API
* ARGO-2619 Remove Centos 6 RPM building
* ARGO-2656 Downtimes as global tenant resource
* ARGO-2860 Separate connection, parsing and file handling
* ARGO-2861 Switch to async IO operations
* ARGO-3521 Retry on empty responses
* ARGO-3524 Improve exception handling
* ARGO-3528 Verbose retry log messages
