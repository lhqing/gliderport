class Manager:
    """
    Sky manager run on-prime.

    Scan File transfer directory for new jobs and open spot VM via sky to run the jobs
    Manager will not stop until a long time after all jobs are done
    """
