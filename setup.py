from distutils.core import setup
import glob

setup(name='argo-egi-connectors',
      version='1.4.0',
      author='SRCE',
      author_email='dvrcic@srce.hr, lgjenero@gmail.com',
      description='Components generate input for ARGO Compute Engine',
      url='http://argoeu.github.io/guides/sync/',
      package_dir={'argo_egi_connectors': 'modules/'},
      packages=['argo_egi_connectors'],
      data_files=[('/etc/argo-egi-connectors', glob.glob('etc/*.conf')),
                  ('/usr/libexec/argo-egi-connectors', glob.glob('bin/*.py')),
                  ('/etc/argo-egi-connectors/schemas', glob.glob('etc/schemas/*.avsc')),
                  ('/etc/cron.d', glob.glob('cronjobs/*'))])
