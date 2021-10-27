from vqem.vqem_score_session_level import VQEM_SESSION
from vqem.vqem_score_device_level import VQEM_DEVICE
from vqem.vqem_score_account_level import VQEM_ACCOUNT


def main(run_date):

    job_5 = VQEM_SESSION()
    status = job_5.__initial_method__(run_date)

    if status == True:

        print("VQEM on Session Level Job Successfully Completed")
    else:
        print("VQEM on Session Level Job Crashed")

    job_6 = VQEM_DEVICE()
    status = job_6.__initial_method__(run_date)

    if status == True:

        print("VQEM on Device Level Job Successfully Completed")
    else:
        print("VQEM on Device Level Job Crashed")

    job_7 = VQEM_ACCOUNT()
    status = job_7.__initial_method__(run_date)

    if status == True:

        print("VQEM on Account Level Job Successfully Completed")
    else:
        print("VQEM on Account Level Job Crashed")

def run(run_date):

    main(run_date)