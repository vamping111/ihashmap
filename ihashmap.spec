<<<<<<< HEAD
%define package_version %(sed '' .version)

Name:           ihashmap
Version:        %package_version
Release:        1%{dist}
Summary:        Indexed hashmap wrapper in Python

Group:          Libraries
License:        MIT
Source:         %{name}-%{version}.tar.gz
=======
Name:           ihashmap
Version:        0.1
Release:        1%{?dist}
Summary:        Indexed hashmap wrapper in Python

Group:          $REPO_GROUP
License:        MIT
URL:            $REPO_URL
Source0:        $REPO_BUILD_TAR
>>>>>>> 506fbc6... spec: add spec and build github action

BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires:  python3-devel

%description
Automaticly indexed hashmap for quick search and wrapper for things that don't expose .keys method

%prep
%setup -q

<<<<<<< HEAD
%install
export PYTHONPATH="%{buildroot}%{python3_sitelib}"
python3 setup.py install --root="%{buildroot}" --single-version-externally-managed
=======
%build
%pyproject_wheel

%install
%pyproject_install
>>>>>>> 506fbc6... spec: add spec and build github action

%clean
rm -rf $RPM_BUILD_ROOT

%files
<<<<<<< HEAD
%{python3_sitelib}/%{name}/
%{python3_sitelib}/%{name}-%{version}-py%{python3_version}.egg-info/
=======
%{python3_sitelib}/ihashmap/
>>>>>>> 506fbc6... spec: add spec and build github action
