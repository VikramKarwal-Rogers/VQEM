from vqem.vqem_score_session_level import VQEM_SESSION


def main():

    job_5 = VQEM_SESSION()
    status = job_5.__initial_method__()

    if status == True:

        print("VQEM on Session Level Job Successfully Completed")
    else:
        print("VQEM on Session Level Job Crashed")

def run():

    main()