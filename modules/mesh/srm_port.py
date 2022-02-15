def load_srm_port_map(logger, ldap_data, attribute_name):
    """
        Returnes a dictionary which maps hostnames to their respective ldap port if such exists
    """
    port_dict = {}
    for res in ldap_data:
        try:
            attribute = res[attribute_name][0]
            start_index = attribute.index('//')
            colon_index = attribute.index(':', start_index)
            end_index = attribute.index('/', colon_index)
            fqdn = attribute[start_index + 2:colon_index]
            port = attribute[colon_index + 1:end_index]

            port_dict[fqdn] = port

        except ValueError:
            logger.error('Exception happened while retrieving port from: %s' % res)

    return port_dict

def attach_srmport_topodata(logger, attributes, topodata, group_endpoints):
    """
        Get SRM ports from LDAP and put them under tags -> info_srm_port
    """
    srm_port_map = load_srm_port_map(logger, topodata, attributes)
    for endpoint in group_endpoints:
        if endpoint['service'] == 'SRM' and srm_port_map.get(endpoint['hostname'], False):
            endpoint['tags']['info_bdii_SRM2_PORT'] = srm_port_map[endpoint['hostname']]
