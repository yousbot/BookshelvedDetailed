import os
import json
import multiprocessing


def split_json(input_file, output_directory, chunk_size):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    file_index = 1
    line_count = 0
    output_file = None

    for line in lines:
        if line_count % chunk_size == 0:
            if output_file:
                output_file.close()
            output_file = open(os.path.join(output_directory, f'output_{file_index}.json'), 'w')
            file_index += 1

        output_file.write(line)
        line_count += 1

    if output_file:
        output_file.close()


def split_json_worker(args):
    split_json(*args)


def split_json_parallel(input_file, output_directory, chunk_size, num_processes):
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(split_json_worker, [(input_file, output_directory, chunk_size)] * num_processes)


# Example usage
input_file = '/Users/youssef/Desktop/Bookshelved/'
output_directory = 'output_json'
chunk_size = 6250
num_processes = 8

if __name__ == '__main__':
    split_json_parallel(input_file, output_directory, chunk_size, num_processes)
