def filter_dups_noemails(contact_list):
    # does not preserve order needed for test
    # no_dups = list(orderedset(contact_list))
    only_emails = list()
    no_dups = list()
    visited = set()
    for contact in contact_list:
        if contact in visited:
            continue
        else:
            no_dups.append(contact)
        visited.add(contact)

    for contact in no_dups:
        if '@' not in contact:
            continue

        if ',' in contact:
            only_emails = only_emails + contact.split(',')
        elif ';' in contact:
            only_emails = only_emails + contact.split(';')
        else:
            only_emails.append(contact)

    return only_emails


def attach_contacts_topodata(logger, contacts, topodata):
    updated_topodata = list()
    found_contacts = None

    if len(contacts) == 0:
        return topodata

    try:
        for entity in topodata:
            # group_groups topotype
            if 'subgroup' in entity:
                found_contacts = list(
                    filter(lambda contact:
                        contact['name'] == entity['subgroup'],
                        contacts)
                )
                if found_contacts:
                    emails = list()
                    for contact in found_contacts[0]['contacts']:
                        if isinstance(contact, str):
                            filtered_emails = filter_dups_noemails(found_contacts[0]['contacts'])
                            if filtered_emails:
                                entity.update(notifications={
                                    'contacts': filtered_emails,
                                    'enabled': True
                                })
                                break
                        else:
                            emails.append(contact['email'])
                    if emails:
                        filtered_emails = filter_dups_noemails(emails)
                        if filtered_emails:
                            entity.update(notifications={
                                'contacts': filtered_emails,
                                'enabled': True
                            })

            # group_endpoints topotype
            else:
                for contact in contacts:
                    fqdn, servtype = contact['name'].split('+')
                    emails = filter_dups_noemails(contact['contacts'])
                    found_endpoints = list(
                        filter(lambda endpoint:
                            endpoint['hostname'] == fqdn \
                            and endpoint['service'] == servtype,
                            topodata)
                    )
                    if emails:
                        for endpoint in found_endpoints:
                            endpoint.update(notifications={
                                'contacts': emails,
                                'enabled': True
                            })

            updated_topodata.append(entity)

    except (KeyError, ValueError, TypeError) as exc:
        logger.warn('Error joining contacts and topology data: %s' % repr(exc))
        if entity:
            logger.warn('Topology entity: %s' % entity)
        if found_contacts:
            logger.warn('Found contacts: %s' % found_contacts)

    return updated_topodata
