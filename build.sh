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
mkdir -p gobblin-PNDA-${VERSION}
./gradlew clean findBugsMain pmdMain build -Pversion=${VERSION}
cp -r build/libs/* gobblin-PNDA-${VERSION}
cp -r tmp/libs/* gobblin-PNDA-${VERSION}
tar -zcf gobblin-PNDA-${VERSION}.tar.gz gobblin-PNDA-${VERSION}
rm -rf tmp
rm -rf gobblin-PNDA-${VERSION}
mv gobblin-PNDA-${VERSION}.tar.gz pnda-build/
sha512sum pnda-build/gobblin-PNDA-${VERSION}.tar.gz > pnda-build/gobblin-PNDA-${VERSION}.tar.gz.sha512.txt
