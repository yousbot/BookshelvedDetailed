def get_different_lines(file1_path, file2_path, output_path):
    # Read the contents of both files
    with open(file1_path, "r") as file1:
        lines1 = file1.readlines()
    with open(file2_path, "r") as file2:
        lines2 = file2.readlines()

    # Find the different lines
    different_lines = set(lines1).symmetric_difference(lines2)

    # Write the different lines to the output file
    with open(output_path, "w") as output_file:
        for line in different_lines:
            output_file.write(line.strip() + '\n')

# Example usage
file1_path = "/Users/youssef/Desktop/Bookshelved/BackUp/book_2_120623_1115.jl"
file2_path = "/Users/youssef/Desktop/Bookshelved/src/BooksInsertedUrls.txt"
output_path = "BooksNotInserted.txt"
get_different_lines(file1_path, file2_path, output_path)
