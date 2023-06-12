import json
import sys

def find_unique_books(file_a, file_b, output_file):
    with open(file_a, 'r') as a_file, open(file_b, 'r') as b_file, open(output_file, 'w') as out_file:
        # Read the lines from file B and create a set of unique book URLs
        book_urls_b = set(line.strip() for line in b_file)

        # Iterate over the lines in file A (JSON format)
        for line_a in a_file:
            data = json.loads(line_a)
            book_url = data.get('url')
            if book_url and book_url not in book_urls_b:
                # Write the line from file A to the output file
                out_file.write(line_a)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python script.py file_a file_b output_file")
        sys.exit(1)

    file_a_path = sys.argv[1]
    file_b_path = sys.argv[2]
    output_file_path = sys.argv[3]

    find_unique_books(file_a_path, file_b_path, output_file_path)
