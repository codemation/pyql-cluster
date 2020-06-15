import sys

if __name__ == '__main__':
    tag = sys.argv[1]
    if len(sys.argv) == 2:
        tagId = 0.001
    else:
        latest = sys.argv[2]
        tagId = float(latest.split(tag)[1])
        tagId+=0.001
    print(f"{tag}{tagId:.2f}")