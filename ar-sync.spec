Name: ar-sync
Summary: A/R Comp Engine sync scripts
Version: 1.3.1
Release: 2%{?dist}
License: ASL 2.0
Buildroot: %{_tmppath}/%{name}-buildroot
Group:     EGI/SA4
BuildArch: noarch
Source0:   %{name}-%{version}.tar.gz
Requires:  crontabs, anacron
Requires:  hive, avro

%description
Installs the service for syncing A/R Comp Engine
with SAM topology and POEM definitions per day.

%prep
%setup 

%install 
%{__rm} -rf %{buildroot}
install --directory %{buildroot}/usr/libexec/ar-sync
install --directory %{buildroot}/etc/ar-sync/
install --directory %{buildroot}/var/lib/ar-sync
install --directory %{buildroot}/var/log/ar-sync
install --directory %{buildroot}/etc/cron.d
install --mode 644 etc/ar-sync/poem-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem-profile.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem-server.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/topology-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/downtime-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/prefilter.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/vo-sync.conf %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/poem_name_mapping.cfg %{buildroot}/var/lib/ar-sync/
install --mode 644 etc/ar-sync/downtimes.avsc %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/metric_profiles.avsc %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/group_endpoints.avsc %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/sites.avsc %{buildroot}/etc/ar-sync/
install --mode 644 etc/ar-sync/weight_sites.avsc %{buildroot}/etc/ar-sync/
install --mode 755 bin/poem-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/topology-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/downtime-sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/hepspec_sync %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/prefilter %{buildroot}/usr/libexec/ar-sync
install --mode 755 bin/vo-sync %{buildroot}/usr/libexec/ar-sync
install --mode 644 cronjobs/poem %{buildroot}/etc/cron.d/poem
install --mode 644 cronjobs/topology %{buildroot}/etc/cron.d/topology
install --mode 644 cronjobs/hepspec %{buildroot}/etc/cron.d/hepspec

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root)
%attr(0755,root,root) /usr/libexec/ar-sync/poem-sync
%attr(0755,root,root) /usr/libexec/ar-sync/topology-sync
%attr(0755,root,root) /usr/libexec/ar-sync/downtime-sync
%attr(0755,root,root) /usr/libexec/ar-sync/hepspec_sync
%attr(0755,root,root) /usr/libexec/ar-sync/prefilter
%attr(0755,root,root) /usr/libexec/ar-sync/vo-sync
%config(noreplace) /etc/ar-sync/poem-sync.conf
%config(noreplace) /etc/ar-sync/poem.conf
%config(noreplace) /etc/ar-sync/poem-profile.conf
%config(noreplace) /etc/ar-sync/poem-server.conf
%config(noreplace) /etc/ar-sync/topology-sync.conf
%config(noreplace) /etc/ar-sync/downtime-sync.conf
%config(noreplace) /etc/ar-sync/prefilter.conf
%config(noreplace) /etc/ar-sync/vo-sync.conf
%config(noreplace) /etc/ar-sync/downtimes.avsc
%config(noreplace) /etc/ar-sync/metric_profiles.avsc
%config(noreplace) /etc/ar-sync/group_endpoints.avsc
%config(noreplace) /etc/ar-sync/sites.avsc
%config(noreplace) /etc/ar-sync/weight_sites.avsc
%attr(0750,root,root) /var/lib/ar-sync
%config(noreplace) /var/lib/ar-sync/poem_name_mapping.cfg
%attr(0750,root,root) /var/log/ar-sync
%attr(0644,root,root) /etc/cron.d/poem
%attr(0644,root,root) /etc/cron.d/topology
%attr(0644,root,root) /etc/cron.d/hepspec

%changelog
* Wed Nov 17 2014 Daniel Vrcic <dvrcic@srce.hr> - 1.3.1-0%{?dist}
- ar-sync is missing avro dependency
- poem-sync is missing data for servers listed in URL
* Thu Nov 27 2014 Luko Gjenero <lgjenero@srce.hr> - 1.3.0-0%{?dist}
- Avro format for poem, downtimes, topology and hepspec
* Tue May 13 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.3-1%{?dist}
- Added logging to sync components
* Fri Apr 26 2014 Luko Gjenero <lgjenero@srce.hr> - 1.2.2-1%{?dist}
- Updated prefilter
* Tue Mar 18 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.2.1-1%{?dist}
- Updated daily cronjobs to run within first five minutes of each day
* Thu Jan 30 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.1.19-1%{?dist}
- Updated daily cronjobs to run within first hour of each day
* Tue Jan 14 2014 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.1.18-1%{?dist}
- Added daily cronjob for hepspec values
* Thu Nov 28 2013 Luko Gjenero <lgjenero@srce.hr> - 1.1.16-3%{?dist}
- Fixed prefilter
* Thu Nov 28 2013 Luko Gjenero <lgjenero@srce.hr> - 1.1.16-2%{?dist}
- Fixed prefilter
* Thu Nov 28 2013 Luko Gjenero <lgjenero@srce.hr> - 1.1.16-1%{?dist}
- Updated prefilter
* Thu Nov 13 2013 Luko Gjenero <lgjenero@srce.hr> - 1.1.15-1%{?dist}
- VO Sync component
* Fri Nov 8 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.1.0-1%{?dist}
- Inclusion of hepspec sync plus cronjobs
* Mon Nov 4 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-6%{?dist}
- Fixes in consumer
* Tue Sep 17 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-5%{?dist}
- Fix in prefilter
* Mon Sep 9 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-4%{?dist}
- Rebuilt with fixed downtimes issue
* Thu Aug 29 2013 Paschalis Korosoglou <pkoro@grid.auth.gr> - 1.0.0-2%{?dist}
- Minor change in prefilter script
* Thu Aug 1 2013 Luko Gjenero <lgjenero@srce.hr> - 1.0.0-1%{?dist}
- Initial release
