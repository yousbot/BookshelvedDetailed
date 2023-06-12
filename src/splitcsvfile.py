import os
import csv
import multiprocessing

def split_csv(input_file, output_directory, chunk_size):
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header line

        file_index = 1
        line_count = 0
        output_file = None

        for row in reader:
            if line_count % chunk_size == 0:
                if output_file:
                    output_file.close()
                output_file = open(os.path.join(output_directory, f'output_{file_index}.csv'), 'w', newline='')
                writer = csv.writer(output_file)
                writer.writerow(header)
                file_index += 1
            writer.writerow(row)
            line_count += 1

        if output_file:
            output_file.close()

def split_csv_worker(args):
    split_csv(*args)

def split_csv_parallel(input_file, output_directory, chunk_size, num_processes):
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(split_csv_worker, [(input_file, output_directory, chunk_size)] * num_processes)

# Example usage
input_file = '/Users/youssef/Desktop/Bookshelved/BackUp/author_120623_0000.jl'
output_directory = 'outputcsv'
chunk_size = 6250
num_processes = 4

if __name__ == '__main__':
    split_csv_parallel(input_file, output_directory, chunk_size, num_processes)
