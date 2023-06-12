import json
import sys

def filter_books_with_data(input_file, output_file):
    with open(input_file, "r") as file_in, open(output_file, "w") as file_out:
        for line in file_in:
            book_data = json.loads(line.strip())
            if len(book_data) > 1:  # Check if book data contains more than just the URL
                json.dump(book_data, file_out)
                file_out.write("\n")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please provide input and output file paths.")
        print("Usage: python script.py input_file.txt output_file.txt")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    filter_books_with_data(input_file, output_file)
