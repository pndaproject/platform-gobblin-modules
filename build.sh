#!/bin/bash
#
# Please check pnda-build/ for the build products

VERSION=${1}

function error {
    echo "Not Found"
    echo "Please run the build dependency installer script"
    exit -1
}

function code_quality_error {
    echo "${1}"
}


BASE=${PWD}

# echo -n "Code quality: "
# Add Functionality to check Java Code Quality

# Unit tests
# call to unit-test the platform-gobblin-modules

cd ${BASE}

# Build
mkdir -p pnda-build
./gradlew clean findBugsMain pmdMain build -Pversion=${VERSION}
cd build/libs
tar -cvf gobblin-PNDA-${VERSION}.tar.gz *
cd $BASE
mv build/libs/gobblin-PNDA-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/gobblin-PNDA-${VERSION}.tar.gz > pnda-build/gobblin-PNDA-${VERSION}.tar.gz.sha512.txt
