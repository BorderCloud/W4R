#!/bin/bash

#chmod +x wikidata_import_dump.sh
# ./wikidata_import_dump.sh

wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
bunzip2 latest-all.json.bz2

