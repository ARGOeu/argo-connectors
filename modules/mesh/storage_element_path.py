def extract_value(key, entry):
    if isinstance(entry, tuple):
        for e in entry:
            k, v = e[0]
            if key == k:
                return v
    else:
        return entry.get(key, None)


def update_map_entry(endpoint, mapping, sepath, voname):
    if endpoint not in mapping:
        mapping[endpoint] = list()
        mapping[endpoint].append({
            'voname': voname,
            'GlueVOInfoPath': sepath
        })
    else:
        mapping[endpoint].append({
            'voname': voname,
            'GlueVOInfoPath': sepath
        })


def ispath_already_added(endpoint, mapping, sepath):
    target = None

    if endpoint in mapping:
        target = mapping[endpoint]
        sepaths = set()

        for entry in target:
            sepaths.add(entry['GlueVOInfoPath'])

        return sepath in sepaths

    else:
        return False


def build_map_endpoint_path(logger, bdiidata):
    mapping = dict()

    try:
        for entry in bdiidata:
            voname = extract_value('GlueVOInfoAccessControlBaseRule', entry)
            if isinstance(voname, list):
                voname = list(filter(lambda e: 'VO:' in e, voname))
                if voname:
                    voname = voname[0].split(':')[1]
                else:
                    continue

            sepath = extract_value('GlueVOInfoPath', entry)
            sepath = sepath[0] if isinstance(sepath, list) else None
            endpoint = extract_value('GlueSEUniqueID', entry['dn'].rdns)

            if (voname and sepath and endpoint
                and not ispath_already_added(endpoint, mapping, sepath)
                and ' ' not in voname):
                update_map_entry(endpoint, mapping, sepath, voname)

            elif (voname and sepath and endpoint
                and not ispath_already_added(endpoint, mapping, sepath)
                and ' ' in voname):
                vonames = voname.split(' ')
                for vo in vonames:
                    update_map_entry(endpoint, mapping, sepath, vo)

    except IndexError as exc:
        logger.error('Error building map of endpoints and storage paths from BDII data: %s' % repr(exc))
        logger.error('LDAP entry: %s' % entry)

    return mapping


def attach_sepath_topodata(logger, bdii_opts, bdiidata, group_endpoints):
    """
        Get SRM ports from LDAP and put them under tags -> info_srm_port
    """
    endpoint_sepaths = build_map_endpoint_path(logger, bdiidata)

    for endpoint in group_endpoints:
        if endpoint['hostname'] in endpoint_sepaths:
            for paths in endpoint_sepaths[endpoint['hostname']]:
                voname = paths['voname']
                sepath = paths['GlueVOInfoPath']
                endpoint['tags'].update({
                    'vo_{}_attr_SE_PATH'.format(voname): sepath
                })
