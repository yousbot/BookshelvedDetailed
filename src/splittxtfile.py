import os
import multiprocessing


def split_file(input_file, output_directory, chunk_size):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    file_index = 1
    line_count = 0
    output_file = None

    for line in lines:
        if line_count % chunk_size == 0:
            if output_file:
                output_file.close()
            output_file = open(os.path.join(output_directory, f'output_{file_index}.txt'), 'w')
            file_index += 1

        output_file.write(line)
        line_count += 1

    if output_file:
        output_file.close()


def split_file_worker(args):
    split_file(*args)


def split_file_parallel(input_file, output_directory, chunk_size, num_processes):
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(split_file_worker, [(input_file, output_directory, chunk_size)] * num_processes)


# Example usage
input_file = '/Users/youssef/Desktop/Bookshelved/src/CleanFailedUrls.txt'
output_directory = 'output_books_data'
chunk_size = 6000
num_processes = 4

if __name__ == '__main__':
    split_file_parallel(input_file, output_directory, chunk_size, num_processes)
