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


def attach_contacts_topodata(logger, contacts, topodata, notification_flag=None):
    updated_topodata = list()
    found_contacts = None

    if len(contacts) == 0:
        return topodata

    try:
        for entity in topodata:
            # group_groups topotype
            if 'subgroup' in entity:
                if entity['subgroup'] in contacts:
                    emails = list()
                    for contact in contacts[entity['subgroup']]:
                        if isinstance(contact, str):
                            filtered_emails = filter_dups_noemails(contacts[entity['subgroup']])
                            if filtered_emails:
                                entity.update(notifications={
                                    'contacts': filtered_emails,
                                    'enabled': entity['notifications']['enabled'] \
                                        if notification_flag else True
                                })
                                break
                        else:
                            emails.append(contact['email'])
                    if emails:
                        filtered_emails = filter_dups_noemails(emails)
                        if filtered_emails:
                            entity.update(notifications={
                                'contacts': filtered_emails,
                                'enabled': entity['notifications']['enabled'] \
                                    if notification_flag else True
                            })

            # group_endpoints topotype
            else:
                contact_key = None

                lookup_key = entity['hostname'].replace('_', '+', 1)
                if lookup_key in contacts:
                    contact_key = lookup_key
                else:
                    lookup_key = '{}+{}'.format(entity['hostname'], entity['service'])
                    if lookup_key in contacts:
                        contact_key = lookup_key

                if contact_key:
                    contact = contacts[contact_key]
                    entity.update(notifications={
                        'contacts': contact,
                        'enabled': entity['notifications']['enabled'] \
                            if notification_flag else True
                    })

            updated_topodata.append(entity)

    except (KeyError, ValueError, TypeError) as exc:
        logger.warn('Error joining contacts and topology data: %s' % repr(exc))
        if entity:
            logger.warn('Topology entity: %s' % entity)
        if found_contacts:
            logger.warn('Found contacts: %s' % found_contacts)

    return updated_topodata
