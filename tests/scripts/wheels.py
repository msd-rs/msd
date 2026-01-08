from polars._utils.logging import eprint
import sys
import glob


def main():
  # argv[1] be like "v0.1.10", by git tag
  version = sys.argv[1][1:]
  target = "./target"
  pattern = f"{target}/wheels/msd-{version}*.whl"
  wheels = glob.glob(pattern)

  eprint("Pattern: ", pattern)
  eprint("Wheels: ", wheels)

  print(f"MSD_WHEELS={','.join(wheels)}")


if __name__ == "__main__":
  main()
