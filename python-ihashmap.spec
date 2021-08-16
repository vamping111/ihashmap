%define package_version %(sed '' .version)

Name:           ihashmap
Version:        %package_version
Release:        1%{dist}
Summary:        Indexed hashmap wrapper in Python

Group:          Libraries
License:        MIT
Source:         %{name}-%{version}.tar.gz

BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  python3-devel

%description
Automaticly indexed hashmap for quick search and wrapper for things that don't expose .keys method

%prep
%setup -q

%install
export PYTHONPATH="%{buildroot}%{python3_sitelib}"
python3 setup.py install --root="%{buildroot}" --single-version-externally-managed

%clean
rm -rf $RPM_BUILD_ROOT

%files
%{python3_sitelib}/%{name}/
%{python3_sitelib}/%{name}-%{version}-py%{python3_version}.egg-info/
