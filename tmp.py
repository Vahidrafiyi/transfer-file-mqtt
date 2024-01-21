import time
import sys

def loading_dots():
    for _ in range(3):
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(1)

if __name__ == "__main__":
    print("Loading", end=" ")
    loading_dots()
    print("\nLoading complete!")
