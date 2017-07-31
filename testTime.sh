#!/bin/bash

voms-proxy-init

if [ !-z ${CMSSW_VERSION+x} ]
then
    echo "CMSSW env not set, set it first"
    exit 1
fi

cp resources/*of5 .

if [[ -e ../results/${1} ]]
then
echo "deleting ../results/${1}"
rm -rf ../results/${1}
fi
mkdir ../results/${1}
mkdir ../results/${1}/scheduler
mkdir ../results/${1}/matrix

cp resources/*of5 .

export wfs=`cat $1`

echo "print wfs comma:"
echo $wfs

date > ../results/${1}/scheduler/start_time

python prepareSteps.py -l $wfs
python main.py -a $SCRAM_ARCH -r ${CMSSW_VERSION:0:11} -d 7

mv ../${CMSSW_VERSION}/pyRelval/* ../results/${1}/scheduler/
mv jobs_results_ideRun.json ../results/${1}/scheduler/

date > ../results/${1}/scheduler/end_time

date > ../results/${1}/matrix/start_time

python run-ib-relval.py -l $wfs
mv ../${CMSSW_VERSION}/pyRelval/* ../results/${1}/matrix/

date > ../results/${1}/matrix/end_time

