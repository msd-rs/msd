import sys
import glob


def main():
  # argv[1] be like "v0.1.10", by git tag
  version = sys.argv[1][1:]
  target = "./target"
  pattern = f"{target}/wheels/pymsd-{version}*.whl"
  wheels = glob.glob(pattern)

  print("Pattern: ", pattern, file=sys.stderr)
  print("Wheels: ", wheels, file=sys.stderr)

  print(f"MSD_WHEELS={','.join(wheels)}")


if __name__ == "__main__":
  main()
