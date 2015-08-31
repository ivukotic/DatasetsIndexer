#!/bin/bash
# uncomment to debug
# uset -x

export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
export RUCIO_ACCOUNT=ivukotic
localSetupRucioClients

echo "getting info from Rucio..."

WD=/home/ivukotic/DatasetsIndexer
cd $WD
rucio list-datasets-rse MWT2_UC_LOCALGROUPDISK > dsnames.txt
python Indexer.py