from canary import Canary
import config
import time


def main():
    canary = Canary()
    while True:
        canary.check_random_annotations()
        time.sleep(config.CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
