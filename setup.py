from distutils.core import setup
import glob

NAME = 'argo-egi-connectors'


def get_ver():
    try:
        for line in open(NAME + '.spec'):
            if "Version:" in line:
                return line.split()[1]
    except IOError:
        print("Make sure that {} is in directory".format(NAME + '.spec'))
        raise SystemExit(1)


setup(name=NAME,
      version=get_ver(),
      author='SRCE',
      author_email='dvrcic@srce.hr, kzailac@srce.hr',
      description='Components generate input data for ARGO Compute Engine',
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: POSIX",
          "Programming Language :: Python :: 3.6",
          "Intended Audience :: Developers",
      ],
      url='http://argoeu.github.io/guides/sync/',
      package_dir={'argo_egi_connectors.io': 'modules/io',
                   'argo_egi_connectors.parse': 'modules/parse',
                   'argo_egi_connectors.mesh': 'modules/mesh',
                   'argo_egi_connectors.tasks': 'modules/tasks',
                   'argo_egi_connectors': 'modules/'},
      packages=['argo_egi_connectors', 'argo_egi_connectors.io',
                'argo_egi_connectors.parse', 'argo_egi_connectors.mesh',
                'argo_egi_connectors.tasks'],
      data_files=[('/etc/argo-egi-connectors', glob.glob('etc/*.conf.template')),
                  ('/usr/libexec/argo-egi-connectors', ['exec/downtimes-gocdb-connector.py',
                                                        'exec/metricprofile-webapi-connector.py',
                                                        'exec/topology-provider-connector.py',
                                                        'exec/topology-gocdb-connector.py',
                                                        'exec/service-types-gocdb-connector.py',
                                                        'exec/service-types-json-connector.py',
                                                        'exec/service-types-csv-connector.py',
                                                        'exec/topology-json-connector.py',
                                                        'exec/weights-vapor-connector.py',
                                                        'exec/topology-csv-connector.py']),
                  ('/etc/argo-egi-connectors/schemas', glob.glob('etc/schemas/*.avsc'))])
