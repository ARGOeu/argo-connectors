import asyncio
from concurrent.futures import ProcessPoolExecutor


def _attach(entries, tags):
    updated_entries = list()
    for entry in entries:
        for tag in tags:
            entry['tags'].update(tag)
        updated_entries.append(entry)
    return updated_entries


async def attach_tags(groups, endpoints, tags_gg, tags_ge):
    loop = asyncio.get_running_loop()

    if tags_gg and tags_ge:
        executor = ProcessPoolExecutor(max_workers=2)
        attach_workers = list()
        attach_workers.append(loop.run_in_executor(executor, _attach, groups, tags_gg))
        attach_workers.append(loop.run_in_executor(executor, _attach, endpoints, tags_ge))

        groups, endpoints = await asyncio.gather(*attach_workers)

        return groups, endpoints

    elif tags_gg:
        changed_groups = _attach(groups, tags_gg)
        return changed_groups, endpoints

    elif tags_ge:
        changed_endpoints = _attach(endpoints, tags_ge)
        return groups, changed_endpoints
