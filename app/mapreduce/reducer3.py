import sys

for line in sys.stdin:
    try:
        print(line.strip())
    except Exception as e:
        sys.stderr.write(f"Error in reducer3: {str(e)}\n")