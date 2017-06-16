Name: tarantool-dump
Version: 1.0.0
Release: 1%{?dist}
Summary: Logical backup and restore for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/dump
Source0: dump-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool-devel >= 1.6.8.0
Requires: tarantool >= 1.6.8.0

%description
This package provides logical dump and restore for Tarantool.

%prep
%setup -q -n dump-%{version}

%check
./test/dump.test.lua

%install
# Create /usr/share/tarantool/dump
mkdir -p %{buildroot}%{_datadir}/tarantool/dump
# Copy init.lua to /usr/share/tarantool/dump/init.lua
cp -p dump/*.lua %{buildroot}%{_datadir}/tarantool/dump

%files
%dir %{_datadir}/tarantool/dump
%{_datadir}/tarantool/dump/
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE AUTHORS

%changelog
* Fri Jun 16 2017 Konstantin Osipov <kostja@taratoool.org> 1.0.0-1
- Initial release
