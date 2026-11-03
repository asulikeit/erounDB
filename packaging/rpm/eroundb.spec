# erounDB RPM (Rocky Linux 9 / RHEL 9). scripts/build-rpm.sh 로 빌드한다.
%global debug_package %{nil}
Name:           eroundb
Version:        %{?jamsil_version}%{!?jamsil_version:0.1.0}
Release:        %{?jamsil_release}%{!?jamsil_release:8}%{?dist}
Summary:        Rust-based distributed relational database (PostgreSQL wire, Raft, SQLite, Tantivy)
License:        Apache-2.0
URL:            https://github.com/CHANGE_ME/erounDB
Source0:        eroundb-%{version}-bin.tar.gz
BuildArch:      x86_64
Requires(pre):  shadow-utils
# 0.1.0-6 이하는 jamsildb 라는 패키지 이름으로 배포되었다. 설치 경로·서비스(jamsildb.service)·계정은 그대로다.
Provides:       jamsildb = %{version}-%{release}
Obsoletes:      jamsildb < 0.1.0-7
%{?systemd_requires}

%description
erounDB node: jamsild supervisor and the jamsil-sql, jamsil-store, jamsil-sync,
jamsil-search and jamsil-log processes. Clients connect with PostgreSQL JDBC/DBeaver on TCP 9786;
nodes replicate with Raft on TCP 9787.

%prep
%setup -q -n eroundb-%{version}

%build
# 바이너리는 cargo build --profile dist 결과를 사용한다.

%install
install -d %{buildroot}%{_bindir}
for b in jamsild jamsil-sql jamsil-store jamsil-sync jamsil-search jamsil-log; do
  install -m 0755 bin/$b %{buildroot}%{_bindir}/$b
done
install -d %{buildroot}%{_sysconfdir}/jamsildb
install -m 0640 config/standalone.toml %{buildroot}%{_sysconfdir}/jamsildb/jamsildb.toml
install -m 0640 config/cluster.toml %{buildroot}%{_sysconfdir}/jamsildb/cluster.toml.example
install -d %{buildroot}%{_unitdir}
install -m 0644 packaging/systemd/jamsildb.service %{buildroot}%{_unitdir}/jamsildb.service
install -d %{buildroot}%{_sysusersdir}
install -m 0644 packaging/systemd/jamsildb.sysusers.conf %{buildroot}%{_sysusersdir}/jamsildb.conf
install -d -m 0700 %{buildroot}%{_sharedstatedir}/jamsildb
install -d -m 0750 %{buildroot}%{_localstatedir}/log/jamsildb
install -d %{buildroot}%{_docdir}/%{name}
install -m 0644 README.md docs/REFERENCE.md docs/DESIGN.md %{buildroot}%{_docdir}/%{name}/
install -d %{buildroot}%{_licensedir}/%{name}
install -m 0644 LICENSE %{buildroot}%{_licensedir}/%{name}/LICENSE

%pre
getent group jamsildb >/dev/null || groupadd -r jamsildb
getent passwd jamsildb >/dev/null || useradd -r -g jamsildb -d %{_sharedstatedir}/jamsildb -s /sbin/nologin -c "erounDB service account" jamsildb
exit 0

%post
%systemd_post jamsildb.service

%preun
%systemd_preun jamsildb.service

%postun
%systemd_postun_with_restart jamsildb.service

%files
%{_bindir}/jamsild
%{_bindir}/jamsil-sql
%{_bindir}/jamsil-store
%{_bindir}/jamsil-sync
%{_bindir}/jamsil-search
%{_bindir}/jamsil-log
%dir %attr(0750,root,jamsildb) %{_sysconfdir}/jamsildb
%config(noreplace) %attr(0640,root,jamsildb) %{_sysconfdir}/jamsildb/jamsildb.toml
%attr(0640,root,jamsildb) %{_sysconfdir}/jamsildb/cluster.toml.example
%{_unitdir}/jamsildb.service
%{_sysusersdir}/jamsildb.conf
%dir %attr(0700,jamsildb,jamsildb) %{_sharedstatedir}/jamsildb
%dir %attr(0750,jamsildb,jamsildb) %{_localstatedir}/log/jamsildb
%license %{_licensedir}/%{name}/LICENSE
%doc %{_docdir}/%{name}/README.md
%doc %{_docdir}/%{name}/REFERENCE.md
%doc %{_docdir}/%{name}/DESIGN.md

%changelog
* Wed Nov 04 2026 erounDB build <build@eroundb.invalid> - 0.1.0-8
- Fix: log records forwarded to jamsil-log used the pre-0.1.0-3 IPC frame header (no "total" field), so
  jamsil-log rejected every record and system.log, cluster.log, query.log and audit.log stayed empty
  (events still went to journald). Regression tests added in jamsil-observability.
* Wed Nov 04 2026 erounDB build <build@eroundb.invalid> - 0.1.0-7
- Licensed under Apache-2.0.
- Distribution renamed to erounDB: the RPM is now "eroundb" (Obsoletes/Provides jamsildb). Binaries, paths
  (/etc/jamsildb, /var/lib/jamsildb), the jamsildb.service unit and the jamsildb account are unchanged.
  Upgrading from the old "jamsildb" package stops the unit when the old package is erased:
  run "systemctl enable --now jamsildb" afterwards (see README).
* Thu Oct 29 2026 erounDB build <build@eroundb.invalid> - 0.1.0-6
- R2-03: TEXT_MATCH/TEXT_SCORE in REPEATABLE READ or SERIALIZABLE transactions is rejected with
  JDB-4105 (SQLSTATE 0A000) instead of joining the current search index with the pinned snapshot
  (which could return rows whose search condition is false in that snapshot).
* Tue Oct 27 2026 erounDB build <build@eroundb.invalid> - 0.1.0-5
- Leader repairs a follower whose data was wiped (node rebuild procedure): a rejection below the
  acknowledged match index resets the peer's progress and resends entries or a snapshot.
  Before, the wiped node waited until the next leader change.
* Tue Oct 27 2026 erounDB build <build@eroundb.invalid> - 0.1.0-4
- Publish applied_index only after the catalog reload of a DDL entry (readers waiting on that index saw the old schema).
- Latency quantile metrics are always listed (NULL when there were no recent observations).
* Tue Oct 27 2026 erounDB build <build@eroundb.invalid> - 0.1.0-3
- Idempotency serialized in the writer with recorded results,
  typed value validation, snapshot isolation levels, savepoint recovery, complete-or-fail search,
  memory budgets, metrics, roles, fault-injection points, leader async fsync and liveness ping.
- IPC and cluster protocol changed: upgrade requires stopping all nodes (no rolling upgrade from 0.1.0-2).
* Mon Oct 26 2026 erounDB build <build@eroundb.invalid> - 0.1.0-2
- VM deployment fixes (cluster_bind fail-fast, election tick ordering).
