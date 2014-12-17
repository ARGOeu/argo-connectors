# EGI Availability & Reliability Sync components

## Setup & Configuration

1. Copy configuration directories
	
	cp -r etc /

2. Configure components

2.1 Downtimes sync component

	Configuration file: **etc/ar-sync/downtime-sync.conf**

	*gocdbHost* - GOCDB hostname, DEFAULT **goc.egi.eu**
	*gocdbUrl*  - GOCDB url, DEFAULT **https://goc.egi.eu/gocdbpi/**
	*hostKey* - Host SSL private key file path, DEFAULT **/etc/grid-security/hostkey.pem**
	*hostCert* - Host SSL certificate file path, DEFAULT **/etc/grid-security/hostcert.pem**
	*outputDir* - Output directory path, DEFAULT **/var/lib/ar-sync**

2.2 POEM profile sync component

	Configuration files: 
		- **etc/ar-sync/poem-profile.conf**
		- **etc/ar-sync/poem-server.conf**
		- **etc/ar-sync/poem-sync.conf**
		- **etc/ar-sync/poem.conf**
		- **etc/ar-sync/poem_name_mapping.cfg**

2.2.1 Main configuration: **etc/ar-sync/poem-sync.conf**

	*poemFile* - Available VOs for POEM profiles file path, DEFAULT **/etc/ar-sync/poem.conf**
	*poemProfileFile* - POEM profiles file path, DEFAULT **/etc/ar-sync/poem-profile.conf**
	*poemServerFile* - POEM profiles file path, DEFAULT **/etc/ar-sync/poem-server.conf**
	*outputDir* - Output directory path, DEFAULT **/var/lib/ar-sync**
	*hostKey* - Host SSL private key file path, DEFAULT **/etc/grid-security/hostkey.pem**
	*hostCert* - Host SSL certificate file path, DEFAULT **/etc/grid-security/hostcert.pem**
	
2.2.2 Available VOs file: **etc/ar-sync/poem.conf**
	
	List available VOs separated by *;*, DEFAULT **grid-monitoring.cern.ch;ops**

2.2.3 
