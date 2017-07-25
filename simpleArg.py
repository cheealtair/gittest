
import sys

if __name__ == '__main__':
    print sys.argv[1]
    sys.argv[1] = 'blah'
    print sys.argv[1]