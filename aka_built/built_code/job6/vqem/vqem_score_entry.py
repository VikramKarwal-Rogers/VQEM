from vqem.vqem_score_session_level import VQEM_SESSION


def main(run_date):

    job_5 = VQEM_SESSION()
    status = job_5.__initial_method__(run_date)

    if status == True:

        print("VQEM on Session Level Job Successfully Completed")
    else:
        print("VQEM on Session Level Job Crashed")

def run(run_date):

    main(run_date)