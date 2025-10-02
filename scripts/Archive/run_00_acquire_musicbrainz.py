# run_step_00.py

from step_00_acquire_musicbrainz import Step00AcquireMusicbrainz

def main():
    step = Step00AcquireMusicbrainz(
        cleanup_archives=False  # Set to True if you want to remove .tar/.bz2 after extraction
    )
    step.run()


if __name__ == "__main__":
    main()
