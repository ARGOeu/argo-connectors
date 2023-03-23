#!/usr/bin/python3

import argparse
import os
import sys

import asyncio
import uvloop

from argo_connectors.singleton_config import ConfigClass
from argo_connectors.config import Global, CustomerConf
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.utils import date_check

logger = None
globopts = {}
custname = ''
isok = True

# GOCDB explicitly says &scope='' for all scopes

def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' %
                     (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def get_bdii_opts(confcust):
    bdii_custopts = confcust._get_cust_options('BDIIOpts')
    if bdii_custopts:
        bdii_complete, missing = confcust.is_complete_bdii(bdii_custopts)
        if not bdii_complete:
            logger.error('%s options incomplete, missing %s' %
                         ('bdii', ' '.join(missing)))
            raise SystemExit(1)
        return bdii_custopts
    else:
        return None


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf',
                        help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf',
                        help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                        help='write data for this date', type=str, required=False)
    args = parser.parse_args()

    # logger = Logger(os.path.basename(sys.argv[0]))

    # fixed_date = None
    # if args.date and date_check(args.date):
    #     fixed_date = args.date

    # confpath = args.gloconf[0] if args.gloconf else None
    # cglob = Global(sys.argv[0], confpath)
    # globopts = cglob.parse()
    # pass_extensions = eval(globopts['GeneralPassExtensions'.lower()])

    # confpath = args.custconf[0] if args.custconf else None
    # confcust = CustomerConf(sys.argv[0], confpath)
    # confcust.parse()
    # confcust.make_dirstruct()
    # confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    
    # topofeed = confcust.get_topofeed()
    # topofeedpaging = confcust.get_topofeedpaging()
    # uidservendp = confcust.get_uidserviceendpoints()
    # topofetchtype = confcust.get_topofetchtype()
    # custname = confcust.get_custname()
    # #logger.customer = custname #TODO: VIDIT DAL NAM TREBA 

    # auth_custopts = confcust.get_authopts()
    # auth_opts = cglob.merge_opts(auth_custopts, 'authentication')

    # auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    # if not auth_complete:
    #     logger.error('%s options incomplete, missing %s' %
    #                  ('authentication', ' '.join(missing)))
    #     raise SystemExit(1)

    # bdii_opts = get_bdii_opts(confcust)
    # # print("bdii_opts: ", bdii_opts) # za EGI: {'bdii': 'True', 'bdiihost': 'bdii.egi.cro-ngi.hr', 'bdiiport': '2170', 'bdiiquerybase': 'o=grid', 'bdiiqueryfiltersrm': '(&(objectClass=GlueService)(|(GlueServiceType=srm_v1)(GlueServiceType=srm)))', 'bdiiqueryattributessrm': 'GlueServiceEndpoint', 'bdiiqueryfiltersepath': '(objectClass=GlueSATop)', 'bdiiqueryattributessepath': 'GlueVOInfoAccessControlBaseRule GlueVOInfoPath'}

    # webapi_opts = get_webapi_opts(cglob, confcust)
    # # print("webapi_opts: ", webapi_opts)  # za EGI: {'webapitoken': '505c3be00e9e30400b72dbfb0c06268aa73f694b', 'webapihost': 'api.devel.argo.grnet.gr'}

    # toposcope = confcust.get_toposcope()
    # topofeedendpoints = confcust.get_topofeedendpoints()
    # topofeedservicegroups = confcust.get_topofeedservicegroups()
    # topofeedsites = confcust.get_topofeedsites()
    # notiflag = confcust.get_notif_flag()

    # if toposcope:
    #     SERVICE_ENDPOINTS_PI = topofeedendpoints + toposcope
    #     SERVICE_GROUPS_PI = topofeedservicegroups + toposcope
    #     SITES_PI = topofeedsites + toposcope

    # else:
    #     SERVICE_ENDPOINTS_PI = topofeedendpoints
    #     SERVICE_GROUPS_PI = topofeedservicegroups
    #     SITES_PI = topofeedsites

    # loop = uvloop.new_event_loop()
    # asyncio.set_event_loop(loop)
    

    ###############################################################################################

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    config = ConfigClass(args)
    fixed_date = config.get_fixed_date()

    ###############################################################################################


    try:
        # task = TaskGocdbTopology(
        #     loop, logger, sys.argv[0], SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI,
        #     SITES_PI, globopts, auth_opts, webapi_opts, bdii_opts, confcust,
        #     custname, topofeed, topofetchtype, fixed_date, uidservendp,
        #     pass_extensions, topofeedpaging, notiflag)

        ###############################################################################################


        task = TaskGocdbTopology(config, loop) #TODO: OVAKO TREBA IZGLEDATI


        ###############################################################################################

        loop.run_until_complete(task.run())



    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(sys.argv[0], globopts, confcust, fixed_date, False)
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()







# print("loop: ", loop)           # <uvloop.Loop running=False closed=False debug=False>
# print("logger: ", logger)       # <argo_connectors.log.Logger object at 0x7f66f53b8940>
# print("sys.argv[0]: ", sys.argv[0]) # /usr/libexec/argo-connectors/topology-gocdb-connector.py
# print("SERVICE_ENDPOINTS_PI: ", SERVICE_ENDPOINTS_PI) # https://goc-sdc.argo.grnet.gr//gocdbpi/private/?method=get_service_endpoint&scope=
# print("SERVICE_GROUPS_PI: ", SERVICE_GROUPS_PI)       # https://goc-sdc.argo.grnet.gr//gocdbpi/private/?method=get_service_group&scope=
# print("SITES_PI: ", SITES_PI)   # https://goc-sdc.argo.grnet.gr//gocdbpi/private/?method=get_site&scope=
# print("globopts: ", globopts)   # {'generalwritejson': 'True', 'generalpublishwebapi': 'False', 'generalpassextensions': 'True', 'generalcompressjson': 'False', 'authenticationhostkey': '/etc/grid-security/hostkey.pem', 'authenticationhostcert': '/etc/grid-security/hostcert.pem', 'authenticationcapath': '/etc/grid-security/certificates', 'authenticationcafile': '/etc/pki/tls/certs/ca-bundle.crt', 'authenticationverifyservercert': 'True', 'authenticationuseplainhttpauth': 'False', 'authenticationhttpuser': 'xxxx', 'authenticationhttppass': 'xxxx', 'connectiontimeout': '180', 'connectionretry': '60', 'connectionsleepretry': '60', 'connectionretryrandom': 'True', 'connectionsleeprandomretrymax': '300', 'inputstatesavedir': '/var/lib/argo-connectors/states/', 'inputstatedays': '3', 'webapihost': 'api.devel.argo.grnet.gr', 'outputtopologygroupofendpoints': 'group_endpoints_DATE.json', 'outputtopologygroupofgroups': 'group_groups_DATE.json'}
# print("auth_opts: ", auth_opts) # {'authenticationhostkey': '/etc/grid-security/hostkey.pem', 'authenticationhostcert': '/etc/grid-security/hostcert.pem', 'authenticationcapath': '/etc/grid-security/certificates', 'authenticationcafile': '/etc/pki/tls/certs/ca-bundle.crt', 'authenticationverifyservercert': 'True', 'authenticationuseplainhttpauth': 'False', 'authenticationhttpuser': 'xxxx', 'authenticationhttppass': 'xxxx'}
# print("webapi_opts: ", webapi_opts) # {'webapitoken': '4473153af6c67a650a74d81d367e9e83f70e2b7b', 'webapihost': 'api.devel.argo.grnet.gr'}
# print("bdii_opts: ", bdii_opts) # None
# print("confcust: ", confcust)   # <argo_connectors.config.CustomerConf object at 0x7f66f53d32b0>
# print("custname: ", custname)   # SDC
# print("topofeed: ", topofeed)   # https://goc-sdc.argo.grnet.gr/
# print("topofetchtype: ", topofetchtype) # ['sites', 'servicegroups']
# print("fixed_date: ", fixed_date)       # None
# print("uidservendp: ", uidservendp)     # False
# print("pass_extensions: ", pass_extensions) # True
# print("topofeedpaging: ", topofeedpaging)   # False
# print("notiflag: ", notiflag)   # True