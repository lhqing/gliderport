# gliderport

Launch jobs on [skypilot](https://skypilot.readthedocs.io/en/latest/)

![gliderport drawio](https://user-images.githubusercontent.com/29302823/192675631-7b290bb4-fc52-419f-8860-830038e8be11.png)

## GliderPort life cycle
1. Preset defines routine workloads in two parts: 1) sky template yaml defines the resource, setup script, file mount of VM to run the jobs, universal to the same kind of jobs; 2) job template config YAML defile the "run" part of the job, specific to each job. 
2. External data prepared for running the jobs. External data may located on-premises, or on cloud, optional
3. GliderPort prepare functions help prepare config files for a set of jobs, including one sky template YAML and multiple job config YAML.
4. GliderPort runtime has a job listener monitor the job YAML files, once detected, it transfer config and job input to GliderPort Runtime Bucket (5) and notify worker manager (6) to start workers.
5. GliderPort runtime creates a bucket to put the job config YAML files per worker, and job input files per job (if there is input files)
6. GliderPort runtime has a worker manager to do two things: 1) execute `sky spot launch` using the SKY_TEMPLATE.yaml, the "run" part of sky YAML file is starting a VM-worker (9), rather than executing specific jobs. VM-worker will monitor and execute actual job command (9); 2) distribute the job config YAML to available workers by put the YAML file into worker config dir (5)
7. Skypilot runtime start spot-VM using the SKY_TEMPLATE.yaml
8. Skypilot spot controller manage spot-VM, recover preemption until VM worker finish or fail (9)
9. In each spot-VM, a GliderPort VM-worker runtime will monitor its worker config dir in GliderPort Runtime Bucket (5), run the next available job. Each job will try RETRY times until success, otherwise mark as fail. VM-worker record the logging information, stderr and stdout to its worker config dir.
10. Job output is transfer to destination as specified in each job config YAML file.
 
