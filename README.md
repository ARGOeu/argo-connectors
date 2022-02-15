`argo-connectors` is bundle of data fetch components that gather insights in
various e-infrastructures for the ARGO monitoring purposes.

It primarily deals with hierarchical topology from GOCDB compatible sources,
but also flat topologies defined in simple CSV or JSON format are supported.
Topology lists endpoints with asociated services running of them that needs to
be inspected as well as contact informations that will be used for alerts. In
case of hierarchical topology, endpoints are organized in sites or
servicegroups which themselves are organized in NGIs or projects.

Next to topology, `argo-connectors` also gather weights and downtimes data.
Weights are Computation Power figures fetched from VAPOR service for each SITE
that are used for A/R calculation of interested NGI. Downtimes data are time
period scheduled for each endpoint under maintenance that will not be taken
into account during A/R calculation.

All collected data is transformed in appropriate format and pushed on daily
basis on the corresponding API method of ARGO-WEB-API service.

More info: http://argoeu.github.io/guides/sync/

