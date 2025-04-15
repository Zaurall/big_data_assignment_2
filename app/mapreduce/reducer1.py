import sys
from collections import defaultdict

def output_term_data(term, term_data):
    for doc_id, positions in term_data.items():
        position_count = len(positions)
        position_list = ','.join(map(str, positions))
        print(f"{term}\t{doc_id}\t{position_count}\t{position_list}")

current_term = None
term_data = defaultdict(list)

for line in sys.stdin:
    try:
        parts = line.strip().split('\t')
        if len(parts) != 3:
            continue
            
        term, doc_id, position = parts
        position = int(position)

        # If we encounter a new term, output previous term's data
        if current_term and current_term != term:
            output_term_data(current_term, term_data)
            term_data = defaultdict(list)
        
        current_term = term
        term_data[doc_id].append(position)

    except Exception as e:
        sys.stderr.write(f"Error in reducer1: {str(e)}\n")

# Process last term
if current_term:
    output_term_data(current_term, term_data)