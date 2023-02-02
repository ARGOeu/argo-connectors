# override so that bytecode compiling is called with python3
%global __python /usr/bin/python3

Name:    argo-connectors
Version: 2.2.2
Release: 1%{?dist}
Group:   EGI/SA4
License: ASL 2.0
Summary: Components fetch and transform data that represents input for ARGO Compute Engine
Url:     https://github.com/ARGOeu/argo-connectors/
Vendor:  SRCE <dvrcic@srce.hr>

Obsoletes: argo-egi-connectors
Prefix:    %{_prefix}

Requires: python3-aiofiles
Requires: python3-aiohttp
Requires: python3-attrs
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
install --directory %{buildroot}/%{_libexecdir}/argo-connectors/

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%config(noreplace) /etc/argo-connectors/*
%attr(0755,root,root) %dir %{_libexecdir}/argo-connectors/
%attr(0755,root,root) %{_libexecdir}/argo-connectors/*.py
%attr(0755,root,root) %{_libexecdir}/argo-connectors/__pycache__/*

%attr(0755,root,root) %dir %{_sharedstatedir}/argo-connectors/
%attr(0755,root,root) %dir %{_localstatedir}/log/argo-connectors/

%changelog
* Wed Feb  1 2023 Daniel Vrcic <dvrcic@srce.hr> - 2.2.2-1%{dist}
- remove python3-avro dependancy
* Tue Jun  7 2022 Daniel Vrcic <dvrcic@srce.hr> - 2.1.0-1%{dist}
- spec changes to reflect new pkg and py module argo-connectors name
* Thu Feb 10 2022 Daniel Vrcic <dvrcic@srce.hr> - 2.0.0-1%{dist}
- release of async-enabled connectors with additional CSV and JSON topologies
