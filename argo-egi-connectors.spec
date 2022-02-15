# override so that bytecode compiling is called with python3
%global __python /usr/bin/python3

Name:    argo-egi-connectors
Version: 2.0.0
Release: 1%{?dist}
Group:   EGI/SA4
License: ASL 2.0
Summary: Components fetch and transform data that represents input for ARGO Compute Engine
Url:     http://argoeu.github.io/guides/sync/
Vendor:  SRCE <dvrcic@srce.hr>, SRCE <kzailac@srce.hr>

Obsoletes: ar-sync
Prefix:    %{_prefix}

Requires: python3-aiofiles
Requires: python3-aiohttp
Requires: python3-attrs
Requires: python3-avro
Requires: python3-requests
Requires: python3-typing-extensions
Requires: python3-uvloop
Requires: python3-bonsai

BuildRequires: python3-devel python3-setuptools

Source0: %{name}-%{version}.tar.gz

BuildArch: noarch
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot

%description
Installs the components for syncing ARGO Compute Engine
with GOCDB, VAPOR and POEM definitions per day.

%prep
%setup -q

%build
%{py3_build}

%install
%{py3_install "--record=INSTALLED_FILES" }
install --directory %{buildroot}/%{_sharedstatedir}/argo-connectors/
install --directory %{buildroot}/%{_localstatedir}/log/argo-connectors/
install --directory %{buildroot}/%{_libexecdir}/argo-egi-connectors/

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%config(noreplace) /etc/argo-egi-connectors/*
%attr(0755,root,root) %dir %{_libexecdir}/argo-egi-connectors/
%attr(0755,root,root) %{_libexecdir}/argo-egi-connectors/*.py
%attr(0755,root,root) %{_libexecdir}/argo-egi-connectors/__pycache__/*

%attr(0755,root,root) %dir %{_sharedstatedir}/argo-connectors/
%attr(0755,root,root) %dir %{_localstatedir}/log/argo-connectors/

%changelog
* Thu Feb 10 2022 Daniel Vrcic <dvrcic@srce.hr> - 2.0.0-1%{dist}
- release of async-enabled connectors with additional CSV and JSON topologies
