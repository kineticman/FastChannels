#!/bin/sh
set -eu

if [ $# -ne 1 ]; then
    echo "Usage: $0 /path/to/epg.xml" >&2
    exit 2
fi

xml_file=$1
dtd_url=${XMLTV_DTD_URL:-https://raw.githubusercontent.com/XMLTV/xmltv/master/xmltv.dtd}
dtd_file=${XMLTV_DTD_FILE:-/tmp/xmltv.dtd}

if ! command -v xmllint >/dev/null 2>&1; then
    echo "xmllint is required. Install libxml2-utils." >&2
    exit 127
fi

if [ ! -s "$dtd_file" ]; then
    curl -fsSL "$dtd_url" -o "$dtd_file"
fi

xmllint --noout --dtdvalid "$dtd_file" "$xml_file"
