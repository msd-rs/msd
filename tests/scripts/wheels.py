import sys
import glob


def main():
  # argv[1] be like "v0.1.10", by git tag
  version = sys.argv[1][1:]
  pattern = f"msd-{version}*.whl"
  target = "./target/wheels/"
  wheels = glob.glob(pattern, root_dir=target)

  print(f"MSD_WHEELS={','.join(wheels)}")


if __name__ == "__main__":
  main()
