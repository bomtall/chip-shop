import os


def main():
    user = os.environ.get("USER")
    print(f"Hello, {user}, from chip-shop!")


if __name__ == "__main__":
    main()
