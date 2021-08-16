%define package_version 1.0.5
%global dist_raw %(%{__grep} -oP "release \\K[0-9]+\\.[0-9]+" /etc/system-release | tr -d ".")
%define pkgname ihashmap
%define buildid @BUILDID@

Name:           python-%{pkgname}
Version:        %package_version
Release:        1%{dist}
Summary:        Indexed hashmap wrapper in Python

Group:          Libraries
License:        MIT
Source:         %{name}-%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python%{python3_pkgversion}-devel python%{python3_pkgversion}-setuptools

Provides:       python-%{pkgname}

%description
Automaticly indexed hashmap for quick search and wrapper for things that don't expose .keys method

%prep
%setup -q

%build
%{py3_build}

%install
%{py3_install}

%clean
rm -rf %buildroot

%files
%{python3_sitelib}/%{pkgname}/
%{python3_sitelib}/%{pkgname}-%{version}-py%{python3_version}.egg-info/
