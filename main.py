import threading
import asyncio

from flask import Flask
from canary import Canary

app = Flask(__name__)
canary_thread_exception = threading.Event()


def run_canary():
    try:
        canary = Canary()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        asyncio.get_event_loop().run_until_complete(canary.run())
    except Exception as e:
        canary.send_slack_notification(f"Canary thread crashed with exception: {e}")
        print(f"Canary thread crashed with exception: {e}")
        canary_thread_exception.set()


@app.route("/health")
def health_check():
    if canary_thread_exception.is_set():
        return "Canary thread crashed", 500
    return "OK", 200


def main():
    canary_thread = threading.Thread(target=run_canary)
    canary_thread.start()

    app.run(host="0.0.0.0", port=80)


if __name__ == "__main__":
    main()
