# CMSCIResourceBalancer
Scheduler handling jobs (putting them on concurent queue for execution) based on the available machine resources.
It can run concurently any ansamble of jobs if they are described as job[id][stepN] where stepN+1 depends on stepN to finish + additional criteria based on metadata for job[id][stepN] where the metadata for the job has to pass a requirement to be put on the execution queue.
Imposible to pass criteria for a job will lead to deadlock, for example if the metadata sets memory used by a job to be greater than the entire machine memory.
This can happen if one is not careful to use metadata from bigger/different machines and run jobs on machine not as big.
