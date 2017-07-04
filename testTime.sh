!#/bin/bash

voms-proxy-init

cp resources/*of5 .

if [[ -e results/${1} ]]
then
echo bla
#rm -rf results/${1}
fi
mkdir results/${1}
mkdir results/${1}/scheduler
mkdir results/${1}/matrix


rm -rf 1* 2* 3* 4* 5* 6* 7* 8* 9*

cp resources/*of5 .

export wfs=`cat $1`

echo "print wfs comma:"
echo $wfs

echo $wfs | tr "," "\n" > resources/wf_test_list
echo $wfs | tr "," "\n" > resources/wf_slc6_530_1of5.txt

#echo "print wfs list:"
#cat resources/wf_slc6_530_1of5.txt

runTheMatrix.py -l $wfs -j 20 -i all --maxSteps=0

date > results/${1}/scheduler/start_${1}

python main.py

date > results/${1}/scheduler/end_${1}

cp -r 1* 2* 3* 4* 5* 6* 7* 8* 9* jobs_results_ideRun.json results/${1}/scheduler

date > results/${1}/matrix/start_runMatrix_${1}

runTheMatrix.py --useInput all --job-reports --command " --customise Validation/Performance/TimeMemorySummary.customiseWithTimeMemorySummary --prefix 'timeout --signal SIGTERM 7200 ' " -t 4 -j 3 -l $wfs
mv 1* 2* 3* 4* 5* 6* 7* 8* 9* results/${1}/matrix/

date > results/${1}/matrix/end_runMatrix_${1}

