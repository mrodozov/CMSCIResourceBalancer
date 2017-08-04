
for i in `cat allSteps.txt`
do
#echo $i
./testTime_scheduler.sh $i
done

for i in `cat allSteps.txt`
do
#echo $i
./testTime_matrix.sh $i
done
