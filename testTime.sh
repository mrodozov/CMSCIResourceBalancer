!#/bin/bash

voms-proxy-init

cp resources/*of5 .

if [[ -e ../results/${1} ]]
then
echo "deleting ../results/${1}"
rm -rf ../results/${1}
fi
mkdir ../results/${1}
mkdir ../results/${1}/scheduler
mkdir ../results/${1}/matrix


#rm -rf 1* 2* 3* 4* 5* 6* 7* 8* 9*

cp resources/*of5 .

export wfs=`cat $1`

echo "print wfs comma:"
echo $wfs

#echo $wfs | tr "," "\n" > resources/wf_test_list
#echo $wfs | tr "," "\n" > resources/wf_slc6_530_1of5.txt

#echo "print wfs list:"
#cat resources/wf_slc6_530_1of5.txt

#runTheMatrix.py -l $wfs -j 20 -i all --maxSteps=0

#scheduler

date > ../results/${1}/scheduler/start_time

python prepareSteps.py -l $wfs
python main.py -a slc6_amd64_gcc630 -r CMSSW_9_3_X -d 7

mv ../CMSSW_9_3_X_2017-07-26-1100/pyRelval/* ../results/${1}/scheduler/
mv jobs_results_ideRun.json ../results/${1}/scheduler/

date > ../results/${1}/scheduler/end_time

#run-ib-relval
#cp -r 1* 2* 3* 4* 5* 6* 7* 8* 9* jobs_results_ideRun.json results/${1}/scheduler

date > ../results/${1}/matrix/start_time

python run-ib-relval.py -l $wfs
mv ../CMSSW_9_3_X_2017-07-26-1100/pyRelval/* ../results/${1}/matrix/

#runTheMatrix.py --useInput all --job-reports --command " --customise Validation/Performance/TimeMemorySummary.customiseWithTimeMemorySummary --prefix 'timeout --signal SIGTERM 7200 ' " -t 4 -j 3 -l $wfs
#mv 1* 2* 3* 4* 5* 6* 7* 8* 9* results/${1}/matrix/

date > ../results/${1}/matrix/end_time

