import os

VERSION = "2.3.0"

build_ver = os.environ.get('BUILD_VER')

if build_ver:
    vernum = build_ver
else:
    vernum = VERSION
