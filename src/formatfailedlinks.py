import concurrent.futures

def wrap_line_with_curly_braces(line):
    return '{"url": "' + line.strip() + '"}\n'

def wrap_lines_with_curly_braces_parallel(input_file, output_file):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        wrapped_lines = list(executor.map(wrap_line_with_curly_braces, lines))

    with open(output_file, 'w') as file:
        file.writelines(wrapped_lines)

# Example usage
input_file = '/Users/youssef/Desktop/Bookshelved/src/list_books_30k.txt'
output_file = 'CleanFailedUrls.txt'

wrap_lines_with_curly_braces_parallel(input_file, output_file)
